package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	router "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	clusterservice "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointservice "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listenerservice "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	routeservice "github.com/envoyproxy/go-control-plane/envoy/service/route/v3"

	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	xds "github.com/envoyproxy/go-control-plane/pkg/server/v3"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	nodeID        = "local-envoy"
	xdsListenAddr = ":18000"
	apiListenAddr = ":8081"
)

var (
	snapshotCache cache.SnapshotCache
	version       uint64
	mu            sync.Mutex

	backends     = map[string]Backend{}
	listeners    = map[string]ListenerSpec{}
	routeConfigs = map[string]RouteConfigSpec{}
)

type BackendEndpoint struct {
	Address  string `json:"address"`
	Port     uint32 `json:"port"`
	Weight   uint32 `json:"weight,omitempty"`
	Priority uint32 `json:"priority,omitempty"`
}

type UpstreamTLS struct {
	Enabled              bool   `json:"enabled"`
	SNI                  string `json:"sni,omitempty"`
	AutoHostSNI          bool   `json:"auto_host_sni,omitempty"`
	AutoSNISANValidation bool   `json:"auto_sni_san_validation,omitempty"`
	TrustedCAFile        string `json:"trusted_ca_file,omitempty"`
	ClientCertFile       string `json:"client_cert_file,omitempty"`
	ClientKeyFile        string `json:"client_key_file,omitempty"`
}

type HealthCheckSpec struct {
	Type               string `json:"type"`
	Path               string `json:"path,omitempty"`
	Host               string `json:"host,omitempty"`
	TimeoutMS          uint32 `json:"timeout_ms,omitempty"`
	IntervalMS         uint32 `json:"interval_ms,omitempty"`
	HealthyThreshold   uint32 `json:"healthy_threshold,omitempty"`
	UnhealthyThreshold uint32 `json:"unhealthy_threshold,omitempty"`
}

type OutlierDetectionSpec struct {
	Consecutive5xx uint32 `json:"consecutive_5xx,omitempty"`
	IntervalMS     uint32 `json:"interval_ms,omitempty"`
	BaseEjectionMS uint32 `json:"base_ejection_time_ms,omitempty"`
	MaxEjectionPct uint32 `json:"max_ejection_percent,omitempty"`
}

type CircuitBreakersThresholdSpec struct {
	Priority            string `json:"priority,omitempty"`
	MaxConnections      uint32 `json:"max_connections,omitempty"`
	MaxPendingRequests  uint32 `json:"max_pending_requests,omitempty"`
	MaxRequests         uint32 `json:"max_requests,omitempty"`
	MaxRetries          uint32 `json:"max_retries,omitempty"`
	MaxConnectionPools  uint32 `json:"max_connection_pools,omitempty"`
	RetryBudgetPercent  uint32 `json:"retry_budget_percent,omitempty"`
	MinRetryConcurrency uint32 `json:"min_retry_concurrency,omitempty"`
	TrackRemaining      bool   `json:"track_remaining,omitempty"`
}

type RetryPolicySpec struct {
	RetryOn                       string   `json:"retry_on,omitempty"`
	NumRetries                    uint32   `json:"num_retries,omitempty"`
	PerTryTimeoutMS               uint32   `json:"per_try_timeout_ms,omitempty"`
	RetriableStatusCodes          []uint32 `json:"retriable_status_codes,omitempty"`
	HostSelectionRetryMaxAttempts uint32   `json:"host_selection_retry_max_attempts,omitempty"`
}

type Backend struct {
	Service          string                         `json:"service"`
	Address          string                         `json:"address,omitempty"`
	IP               string                         `json:"ip,omitempty"`
	Port             uint32                         `json:"port,omitempty"`
	DiscoveryType    string                         `json:"discovery_type,omitempty"`
	Endpoints        []BackendEndpoint              `json:"endpoints,omitempty"`
	LBPolicy         string                         `json:"lb_policy,omitempty"`
	UseHTTP2         bool                           `json:"use_http2,omitempty"`
	UpstreamTLS      *UpstreamTLS                   `json:"upstream_tls,omitempty"`
	HealthCheck      *HealthCheckSpec               `json:"health_check,omitempty"`
	OutlierDetection *OutlierDetectionSpec          `json:"outlier_detection,omitempty"`
	CircuitBreakers  []CircuitBreakersThresholdSpec `json:"circuit_breakers,omitempty"`
}

type ListenerTLS struct {
	Enabled              bool     `json:"enabled"`
	SecretName           string   `json:"secret_name"`
	SDSPath              string   `json:"sds_path"`
	WatchedDirectory     string   `json:"watched_directory"`
	ALPNProtocols        []string `json:"alpn_protocols"`
	RequireClientCert    bool     `json:"require_client_cert"`
	ValidationSecretName string   `json:"validation_secret_name,omitempty"`
	ValidationSDSPath    string   `json:"validation_sds_path,omitempty"`
}

type ListenerSpec struct {
	Name      string       `json:"name"`
	Address   string       `json:"address"`
	Port      uint32       `json:"port"`
	RouteName string       `json:"route_name"`
	Service   string       `json:"service"`
	TLS       *ListenerTLS `json:"tls,omitempty"`
}

type RouteRule struct {
	Prefix             string           `json:"prefix"`
	Cluster            string           `json:"cluster"`
	PrefixRewrite      string           `json:"prefix_rewrite,omitempty"`
	HostRewriteLiteral string           `json:"host_rewrite_literal,omitempty"`
	TimeoutMS          uint32           `json:"timeout_ms,omitempty"`
	RetryPolicy        *RetryPolicySpec `json:"retry_policy,omitempty"`
}

type RouteConfigSpec struct {
	RouteName string      `json:"route_name"`
	Domains   []string    `json:"domains,omitempty"`
	Rules     []RouteRule `json:"rules"`
}

type BackendUpsertRequest = Backend
type ListenerUpsertRequest = ListenerSpec
type RouteUpsertRequest = RouteConfigSpec

type APIResponse struct {
	Message string `json:"message"`
	Version uint64 `json:"version,omitempty"`
}

type StateResponse struct {
	Version      uint64            `json:"version"`
	Backends     []Backend         `json:"backends"`
	Listeners    []ListenerSpec    `json:"listeners"`
	RouteConfigs []RouteConfigSpec `json:"route_configs"`
}

func main() {
	snapshotCache = cache.NewSnapshotCache(false, cache.IDHash{}, nil)
	server := xds.NewServer(context.Background(), snapshotCache, nil)

	if err := rebuildSnapshot(); err != nil {
		log.Fatalf("failed to create initial empty snapshot: %v", err)
	}

	grpcServer := grpc.NewServer()
	discoverygrpc.RegisterAggregatedDiscoveryServiceServer(grpcServer, server)
	endpointservice.RegisterEndpointDiscoveryServiceServer(grpcServer, server)
	clusterservice.RegisterClusterDiscoveryServiceServer(grpcServer, server)
	routeservice.RegisterRouteDiscoveryServiceServer(grpcServer, server)
	listenerservice.RegisterListenerDiscoveryServiceServer(grpcServer, server)

	lis, err := net.Listen("tcp", xdsListenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", xdsListenAddr, err)
	}

	go startHTTPServer()

	log.Printf("xDS control plane listening on %s", xdsListenAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("grpc server failed: %v", err)
	}
}

func startHTTPServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("/clusters/upsert", clusterUpsertHandler)
	mux.HandleFunc("/clusters/delete", clusterDeleteHandler)
	mux.HandleFunc("/clusters", clustersListHandler)

	mux.HandleFunc("/listeners/upsert", listenerUpsertHandler)
	mux.HandleFunc("/listeners/delete", listenerDeleteHandler)
	mux.HandleFunc("/listeners", listenersListHandler)

	mux.HandleFunc("/routes/upsert", routeUpsertHandler)
	mux.HandleFunc("/routes/delete", routeDeleteHandler)
	mux.HandleFunc("/routes", routesListHandler)

	mux.HandleFunc("/state", stateHandler)

	log.Printf("REST API listening on %s", apiListenAddr)
	if err := http.ListenAndServe(apiListenAddr, mux); err != nil {
		log.Fatalf("http server failed: %v", err)
	}
}

func clusterUpsertHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}

	var req BackendUpsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		badRequest(w, "invalid json body: "+err.Error())
		return
	}

	backend, err := normalizeBackend(req)
	if err != nil {
		badRequest(w, err.Error())
		return
	}

	mu.Lock()
	prev, hadPrev := backends[backend.Service]
	backends[backend.Service] = backend

	if err := rebuildSnapshotLocked(); err != nil {
		if hadPrev {
			backends[backend.Service] = prev
		} else {
			delete(backends, backend.Service)
		}
		mu.Unlock()
		internalError(w, "failed to rebuild snapshot: "+err.Error())
		return
	}

	currentVersion := atomic.LoadUint64(&version)
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"message": "cluster upserted",
		"cluster": backend,
		"version": currentVersion,
	})
}

func clusterDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		methodNotAllowed(w)
		return
	}

	service := r.URL.Query().Get("service")
	if service == "" {
		badRequest(w, "service query parameter required")
		return
	}

	mu.Lock()
	backend, ok := backends[service]
	if !ok {
		mu.Unlock()
		writeJSON(w, http.StatusNotFound, APIResponse{Message: "cluster not found"})
		return
	}

	for _, l := range listeners {
		if l.Service == service {
			mu.Unlock()
			badRequest(w, fmt.Sprintf("cluster %q is in use by listener %q", service, l.Name))
			return
		}
	}
	for _, rc := range routeConfigs {
		for _, rule := range rc.Rules {
			if rule.Cluster == service {
				mu.Unlock()
				badRequest(w, fmt.Sprintf("cluster %q is in use by route %q", service, rc.RouteName))
				return
			}
		}
	}

	delete(backends, service)
	if err := rebuildSnapshotLocked(); err != nil {
		backends[service] = backend
		mu.Unlock()
		internalError(w, "failed to rebuild snapshot: "+err.Error())
		return
	}

	currentVersion := atomic.LoadUint64(&version)
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"message": "cluster deleted",
		"cluster": backend,
		"version": currentVersion,
	})
}

func clustersListHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}

	mu.Lock()
	items := make([]Backend, 0, len(backends))
	for _, b := range backends {
		items = append(items, b)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Service < items[j].Service })
	currentVersion := atomic.LoadUint64(&version)
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"clusters": items,
		"version":  currentVersion,
	})
}

func listenerUpsertHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}

	var req ListenerUpsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		badRequest(w, "invalid json body: "+err.Error())
		return
	}

	if req.Name == "" {
		badRequest(w, "name required")
		return
	}
	if req.Address == "" {
		req.Address = "0.0.0.0"
	}
	if req.Port == 0 {
		badRequest(w, "port required")
		return
	}
	if req.RouteName == "" {
		req.RouteName = "route_" + req.Name
	}
	if req.Service == "" {
		badRequest(w, "service required")
		return
	}
	if req.TLS != nil && req.TLS.Enabled {
		normalizeListenerTLS(req.TLS)
	}

	mu.Lock()
	if _, ok := backends[req.Service]; !ok {
		mu.Unlock()
		badRequest(w, fmt.Sprintf("service %q not found", req.Service))
		return
	}

	prev, hadPrev := listeners[req.Name]
	listeners[req.Name] = req
	if err := rebuildSnapshotLocked(); err != nil {
		if hadPrev {
			listeners[req.Name] = prev
		} else {
			delete(listeners, req.Name)
		}
		mu.Unlock()
		internalError(w, "failed to rebuild snapshot: "+err.Error())
		return
	}

	currentVersion := atomic.LoadUint64(&version)
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"message":  "listener upserted",
		"listener": req,
		"version":  currentVersion,
	})
}

func listenerDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		methodNotAllowed(w)
		return
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		badRequest(w, "name query parameter required")
		return
	}

	mu.Lock()
	listener, ok := listeners[name]
	if !ok {
		mu.Unlock()
		writeJSON(w, http.StatusNotFound, APIResponse{Message: "listener not found"})
		return
	}

	delete(listeners, name)
	if err := rebuildSnapshotLocked(); err != nil {
		listeners[name] = listener
		mu.Unlock()
		internalError(w, "failed to rebuild snapshot: "+err.Error())
		return
	}

	currentVersion := atomic.LoadUint64(&version)
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"message":  "listener deleted",
		"listener": listener,
		"version":  currentVersion,
	})
}

func listenersListHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}

	mu.Lock()
	items := make([]ListenerSpec, 0, len(listeners))
	for _, l := range listeners {
		items = append(items, l)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	currentVersion := atomic.LoadUint64(&version)
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"listeners": items,
		"version":   currentVersion,
	})
}

func routeUpsertHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}

	var req RouteUpsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		badRequest(w, "invalid json body: "+err.Error())
		return
	}

	if req.RouteName == "" {
		badRequest(w, "route_name required")
		return
	}
	if len(req.Rules) == 0 {
		badRequest(w, "at least one route rule required")
		return
	}
	if len(req.Domains) == 0 {
		req.Domains = []string{"*"}
	}

	for i := range req.Rules {
		if req.Rules[i].Prefix == "" {
			badRequest(w, fmt.Sprintf("rules[%d].prefix required", i))
			return
		}
		if req.Rules[i].Cluster == "" {
			badRequest(w, fmt.Sprintf("rules[%d].cluster required", i))
			return
		}
	}

	mu.Lock()
	for _, rule := range req.Rules {
		if _, ok := backends[rule.Cluster]; !ok {
			mu.Unlock()
			badRequest(w, fmt.Sprintf("cluster %q not found", rule.Cluster))
			return
		}
	}

	prev, hadPrev := routeConfigs[req.RouteName]
	routeConfigs[req.RouteName] = req
	if err := rebuildSnapshotLocked(); err != nil {
		if hadPrev {
			routeConfigs[req.RouteName] = prev
		} else {
			delete(routeConfigs, req.RouteName)
		}
		mu.Unlock()
		internalError(w, "failed to rebuild snapshot: "+err.Error())
		return
	}

	currentVersion := atomic.LoadUint64(&version)
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"message": "route config upserted",
		"route":   req,
		"version": currentVersion,
	})
}

func routeDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		methodNotAllowed(w)
		return
	}

	name := r.URL.Query().Get("route_name")
	if name == "" {
		badRequest(w, "route_name query parameter required")
		return
	}

	mu.Lock()
	rc, ok := routeConfigs[name]
	if !ok {
		mu.Unlock()
		writeJSON(w, http.StatusNotFound, APIResponse{Message: "route config not found"})
		return
	}
	delete(routeConfigs, name)
	if err := rebuildSnapshotLocked(); err != nil {
		routeConfigs[name] = rc
		mu.Unlock()
		internalError(w, "failed to rebuild snapshot: "+err.Error())
		return
	}

	currentVersion := atomic.LoadUint64(&version)
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"message": "route config deleted",
		"route":   rc,
		"version": currentVersion,
	})
}

func routesListHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}

	mu.Lock()
	items := make([]RouteConfigSpec, 0, len(routeConfigs))
	for _, rc := range routeConfigs {
		items = append(items, rc)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].RouteName < items[j].RouteName })
	currentVersion := atomic.LoadUint64(&version)
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"routes":  items,
		"version": currentVersion,
	})
}

func stateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}

	mu.Lock()
	resp := StateResponse{
		Version:      atomic.LoadUint64(&version),
		Backends:     make([]Backend, 0, len(backends)),
		Listeners:    make([]ListenerSpec, 0, len(listeners)),
		RouteConfigs: make([]RouteConfigSpec, 0, len(routeConfigs)),
	}
	for _, b := range backends {
		resp.Backends = append(resp.Backends, b)
	}
	for _, l := range listeners {
		resp.Listeners = append(resp.Listeners, l)
	}
	for _, rc := range routeConfigs {
		resp.RouteConfigs = append(resp.RouteConfigs, rc)
	}
	sort.Slice(resp.Backends, func(i, j int) bool { return resp.Backends[i].Service < resp.Backends[j].Service })
	sort.Slice(resp.Listeners, func(i, j int) bool { return resp.Listeners[i].Name < resp.Listeners[j].Name })
	sort.Slice(resp.RouteConfigs, func(i, j int) bool { return resp.RouteConfigs[i].RouteName < resp.RouteConfigs[j].RouteName })
	mu.Unlock()

	writeJSON(w, http.StatusOK, resp)
}

func normalizeBackend(req BackendUpsertRequest) (Backend, error) {
	service := strings.TrimSpace(req.Service)
	if service == "" {
		return Backend{}, fmt.Errorf("service required")
	}

	address := strings.TrimSpace(req.Address)
	if address == "" {
		address = strings.TrimSpace(req.IP)
	}

	backend := req
	backend.Service = service
	backend.Address = address
	backend.IP = address

	if len(backend.Endpoints) == 0 {
		if address == "" {
			return Backend{}, fmt.Errorf("address or ip required")
		}
		if backend.Port == 0 {
			return Backend{}, fmt.Errorf("port required")
		}
	}

	if backend.LBPolicy == "" {
		backend.LBPolicy = "ROUND_ROBIN"
	}

	if backend.DiscoveryType == "" {
		if len(backend.Endpoints) > 0 || net.ParseIP(address) != nil {
			backend.DiscoveryType = "EDS"
		} else {
			backend.DiscoveryType = "LOGICAL_DNS"
		}
	}
	backend.DiscoveryType = strings.ToUpper(strings.TrimSpace(backend.DiscoveryType))

	switch backend.DiscoveryType {
	case "EDS":
		if len(backend.Endpoints) == 0 {
			backend.Endpoints = []BackendEndpoint{{
				Address:  address,
				Port:     backend.Port,
				Weight:   1,
				Priority: 0,
			}}
		}
	case "LOGICAL_DNS", "STRICT_DNS":
		if address == "" {
			if len(backend.Endpoints) == 0 {
				return Backend{}, fmt.Errorf("address required for DNS cluster")
			}
			address = backend.Endpoints[0].Address
			backend.Address = address
			backend.IP = address
			if backend.Port == 0 {
				backend.Port = backend.Endpoints[0].Port
			}
		}
		if backend.Port == 0 {
			return Backend{}, fmt.Errorf("port required for DNS cluster")
		}
		if len(backend.Endpoints) > 0 {
			// keep state but cluster will use the first host only for DNS types
		}
	default:
		return Backend{}, fmt.Errorf("unsupported discovery_type %q", backend.DiscoveryType)
	}

	for i := range backend.Endpoints {
		if backend.Endpoints[i].Address == "" {
			return Backend{}, fmt.Errorf("endpoints[%d].address required", i)
		}
		if backend.Endpoints[i].Port == 0 {
			return Backend{}, fmt.Errorf("endpoints[%d].port required", i)
		}
		if backend.Endpoints[i].Weight == 0 {
			backend.Endpoints[i].Weight = 1
		}
	}

	if backend.HealthCheck != nil {
		normalizeHealthCheck(backend.HealthCheck)
	}
	if backend.OutlierDetection != nil {
		normalizeOutlierDetection(backend.OutlierDetection)
	}
	for i := range backend.CircuitBreakers {
		normalizeCircuitBreaker(&backend.CircuitBreakers[i])
	}

	return backend, nil
}

func normalizeHealthCheck(hc *HealthCheckSpec) {
	if hc.Type == "" {
		hc.Type = "http"
	}
	hc.Type = strings.ToLower(hc.Type)
	if hc.TimeoutMS == 0 {
		hc.TimeoutMS = 1000
	}
	if hc.IntervalMS == 0 {
		hc.IntervalMS = 5000
	}
	if hc.HealthyThreshold == 0 {
		hc.HealthyThreshold = 2
	}
	if hc.UnhealthyThreshold == 0 {
		hc.UnhealthyThreshold = 2
	}
	if hc.Path == "" && hc.Type == "http" {
		hc.Path = "/"
	}
}

func normalizeOutlierDetection(od *OutlierDetectionSpec) {
	if od.Consecutive5xx == 0 {
		od.Consecutive5xx = 5
	}
	if od.IntervalMS == 0 {
		od.IntervalMS = 10000
	}
	if od.BaseEjectionMS == 0 {
		od.BaseEjectionMS = 30000
	}
	if od.MaxEjectionPct == 0 {
		od.MaxEjectionPct = 50
	}
}

func normalizeCircuitBreaker(cb *CircuitBreakersThresholdSpec) {
	if cb.Priority == "" {
		cb.Priority = "DEFAULT"
	}
	if cb.RetryBudgetPercent > 0 && cb.MinRetryConcurrency == 0 {
		cb.MinRetryConcurrency = 3
	}
}

func normalizeListenerTLS(tls *ListenerTLS) {
	if tls.SecretName == "" {
		tls.SecretName = "server_cert"
	}
	if tls.SDSPath == "" {
		tls.SDSPath = "./sds.yaml"
	}
	if tls.WatchedDirectory == "" {
		tls.WatchedDirectory = watchedDir(tls.SDSPath, ".")
	}
	if len(tls.ALPNProtocols) == 0 {
		tls.ALPNProtocols = []string{"h2", "http/1.1"}
	}
	if tls.RequireClientCert {
		if tls.ValidationSecretName == "" {
			tls.ValidationSecretName = "validation_context"
		}
		if tls.ValidationSDSPath == "" {
			tls.ValidationSDSPath = tls.SDSPath
		}
	}
}

func rebuildSnapshot() error {
	mu.Lock()
	defer mu.Unlock()
	return rebuildSnapshotLocked()
}

func rebuildSnapshotLocked() error {
	clusterResources := make([]types.Resource, 0, len(backends))
	endpointResources := make([]types.Resource, 0, len(backends))
	routeResources := make([]types.Resource, 0, len(listeners))
	listenerResources := make([]types.Resource, 0, len(listeners))

	for _, b := range backends {
		clusterResources = append(clusterResources, buildCluster(b))
		if strings.EqualFold(b.DiscoveryType, "EDS") {
			endpointResources = append(endpointResources, buildEndpointAssignment(b))
		}
	}

	for _, l := range listeners {
		if _, ok := backends[l.Service]; !ok {
			return fmt.Errorf("listener %q references missing cluster %q", l.Name, l.Service)
		}
		routeResource, err := buildRouteForListener(l)
		if err != nil {
			return err
		}
		routeResources = append(routeResources, routeResource)

		listenerResource, err := buildListener(l)
		if err != nil {
			return fmt.Errorf("build listener %q: %w", l.Name, err)
		}
		listenerResources = append(listenerResources, listenerResource)
	}

	nextVersion := atomic.AddUint64(&version, 1)

	snapshot, err := cache.NewSnapshot(
		fmt.Sprintf("%d", nextVersion),
		map[resource.Type][]types.Resource{
			resource.ClusterType:  clusterResources,
			resource.EndpointType: endpointResources,
			resource.RouteType:    routeResources,
			resource.ListenerType: listenerResources,
		},
	)
	if err != nil {
		atomic.AddUint64(&version, ^uint64(0))
		return fmt.Errorf("create snapshot: %w", err)
	}

	if err := snapshot.Consistent(); err != nil {
		atomic.AddUint64(&version, ^uint64(0))
		return fmt.Errorf("snapshot inconsistent: %w", err)
	}

	if err := snapshotCache.SetSnapshot(context.Background(), nodeID, snapshot); err != nil {
		atomic.AddUint64(&version, ^uint64(0))
		return fmt.Errorf("set snapshot: %w", err)
	}

	log.Printf(
		"snapshot pushed: version=%d clusters=%d listeners=%d routes=%d endpoints=%d",
		nextVersion,
		len(clusterResources),
		len(listenerResources),
		len(routeResources),
		len(endpointResources),
	)

	return nil
}

func buildCluster(spec Backend) *cluster.Cluster {
	c := &cluster.Cluster{
		Name:           spec.Service,
		ConnectTimeout: durationpb.New(5 * time.Second),
		LbPolicy:       mapLBPolicy(spec.LBPolicy),
	}

	if spec.UseHTTP2 {
		c.Http2ProtocolOptions = &core.Http2ProtocolOptions{}
	}

	switch strings.ToUpper(spec.DiscoveryType) {
	case "LOGICAL_DNS":
		c.ClusterDiscoveryType = &cluster.Cluster_Type{Type: cluster.Cluster_LOGICAL_DNS}
		c.LoadAssignment = buildDNSLoadAssignment(spec)
	case "STRICT_DNS":
		c.ClusterDiscoveryType = &cluster.Cluster_Type{Type: cluster.Cluster_STRICT_DNS}
		c.LoadAssignment = buildDNSLoadAssignment(spec)
	default:
		c.ClusterDiscoveryType = &cluster.Cluster_Type{Type: cluster.Cluster_EDS}
		c.EdsClusterConfig = &cluster.Cluster_EdsClusterConfig{
			EdsConfig: &core.ConfigSource{
				ResourceApiVersion: resource.DefaultAPIVersion,
				ConfigSourceSpecifier: &core.ConfigSource_Ads{
					Ads: &core.AggregatedConfigSource{},
				},
			},
		}
	}

	if spec.UpstreamTLS != nil && spec.UpstreamTLS.Enabled {
		if transportSocket, err := buildUpstreamTLSTransportSocket(*spec.UpstreamTLS); err == nil {
			c.TransportSocket = transportSocket
		}
	}

	if spec.HealthCheck != nil {
		c.HealthChecks = []*core.HealthCheck{buildHealthCheck(*spec.HealthCheck)}
	}
	if spec.OutlierDetection != nil {
		c.OutlierDetection = buildOutlierDetection(*spec.OutlierDetection)
	}
	if len(spec.CircuitBreakers) > 0 {
		c.CircuitBreakers = buildCircuitBreakers(spec.CircuitBreakers)
	}

	return c
}

func buildDNSLoadAssignment(spec Backend) *endpointv3.ClusterLoadAssignment {
	address := spec.Address
	port := spec.Port
	if address == "" && len(spec.Endpoints) > 0 {
		address = spec.Endpoints[0].Address
		port = spec.Endpoints[0].Port
	}

	return &endpointv3.ClusterLoadAssignment{
		ClusterName: spec.Service,
		Endpoints: []*endpointv3.LocalityLbEndpoints{
			{
				LbEndpoints: []*endpointv3.LbEndpoint{
					{
						HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
							Endpoint: &endpointv3.Endpoint{
								Address: socketAddress(address, port),
							},
						},
					},
				},
			},
		},
	}
}

func buildEndpointAssignment(spec Backend) *endpointv3.ClusterLoadAssignment {
	byPriority := map[uint32][]*endpointv3.LbEndpoint{}
	priorities := make([]int, 0)

	seen := map[uint32]bool{}
	for _, ep := range spec.Endpoints {
		if !seen[ep.Priority] {
			priorities = append(priorities, int(ep.Priority))
			seen[ep.Priority] = true
		}
		lbEp := &endpointv3.LbEndpoint{
			HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
				Endpoint: &endpointv3.Endpoint{
					Address: socketAddress(ep.Address, ep.Port),
				},
			},
		}
		if ep.Weight > 0 {
			lbEp.LoadBalancingWeight = &wrapperspb.UInt32Value{Value: ep.Weight}
		}
		byPriority[ep.Priority] = append(byPriority[ep.Priority], lbEp)
	}

	sort.Ints(priorities)

	localities := make([]*endpointv3.LocalityLbEndpoints, 0, len(priorities))
	for _, p := range priorities {
		prio := uint32(p)
		localities = append(localities, &endpointv3.LocalityLbEndpoints{
			Priority:    prio,
			LbEndpoints: byPriority[prio],
		})
	}

	return &endpointv3.ClusterLoadAssignment{
		ClusterName: spec.Service,
		Endpoints:   localities,
	}
}

func buildHealthCheck(spec HealthCheckSpec) *core.HealthCheck {
	hc := &core.HealthCheck{
		Timeout:            durationpb.New(time.Duration(spec.TimeoutMS) * time.Millisecond),
		Interval:           durationpb.New(time.Duration(spec.IntervalMS) * time.Millisecond),
		HealthyThreshold:   &wrapperspb.UInt32Value{Value: spec.HealthyThreshold},
		UnhealthyThreshold: &wrapperspb.UInt32Value{Value: spec.UnhealthyThreshold},
	}

	switch spec.Type {
	case "tcp":
		hc.HealthChecker = &core.HealthCheck_TcpHealthCheck_{
			TcpHealthCheck: &core.HealthCheck_TcpHealthCheck{},
		}
	default:
		hc.HealthChecker = &core.HealthCheck_HttpHealthCheck_{
			HttpHealthCheck: &core.HealthCheck_HttpHealthCheck{
				Path: spec.Path,
				Host: spec.Host,
			},
		}
	}

	return hc
}

func buildOutlierDetection(spec OutlierDetectionSpec) *cluster.OutlierDetection {
	return &cluster.OutlierDetection{
		Consecutive_5Xx:    &wrapperspb.UInt32Value{Value: spec.Consecutive5xx},
		Interval:           durationpb.New(time.Duration(spec.IntervalMS) * time.Millisecond),
		BaseEjectionTime:   durationpb.New(time.Duration(spec.BaseEjectionMS) * time.Millisecond),
		MaxEjectionPercent: &wrapperspb.UInt32Value{Value: spec.MaxEjectionPct},
	}
}

func buildCircuitBreakers(specs []CircuitBreakersThresholdSpec) *cluster.CircuitBreakers {
	thresholds := make([]*cluster.CircuitBreakers_Thresholds, 0, len(specs))
	for _, spec := range specs {
		threshold := &cluster.CircuitBreakers_Thresholds{
			Priority:       mapRoutingPriority(spec.Priority),
			TrackRemaining: spec.TrackRemaining,
		}
		if spec.MaxConnections > 0 {
			threshold.MaxConnections = &wrapperspb.UInt32Value{Value: spec.MaxConnections}
		}
		if spec.MaxPendingRequests > 0 {
			threshold.MaxPendingRequests = &wrapperspb.UInt32Value{Value: spec.MaxPendingRequests}
		}
		if spec.MaxRequests > 0 {
			threshold.MaxRequests = &wrapperspb.UInt32Value{Value: spec.MaxRequests}
		}
		if spec.MaxRetries > 0 {
			threshold.MaxRetries = &wrapperspb.UInt32Value{Value: spec.MaxRetries}
		}
		if spec.MaxConnectionPools > 0 {
			threshold.MaxConnectionPools = &wrapperspb.UInt32Value{Value: spec.MaxConnectionPools}
		}
		if spec.RetryBudgetPercent > 0 {
			threshold.RetryBudget = &cluster.CircuitBreakers_Thresholds_RetryBudget{
				BudgetPercent:       &typev3.Percent{Value: float64(spec.RetryBudgetPercent)},
				MinRetryConcurrency: &wrapperspb.UInt32Value{Value: spec.MinRetryConcurrency},
			}
		}
		thresholds = append(thresholds, threshold)
	}
	return &cluster.CircuitBreakers{Thresholds: thresholds}
}

func buildRouteForListener(listener ListenerSpec) (*routev3.RouteConfiguration, error) {
	if cfg, ok := routeConfigs[listener.RouteName]; ok && len(cfg.Rules) > 0 {
		return buildRouteFromSpec(cfg)
	}
	return buildDefaultRoute(listener.RouteName, listener.Service), nil
}

func buildDefaultRoute(routeName, service string) *routev3.RouteConfiguration {
	return &routev3.RouteConfiguration{
		Name: routeName,
		VirtualHosts: []*routev3.VirtualHost{
			{
				Name:    "backend",
				Domains: []string{"*"},
				Routes: []*routev3.Route{
					{
						Match: &routev3.RouteMatch{
							PathSpecifier: &routev3.RouteMatch_Prefix{
								Prefix: "/",
							},
						},
						Action: &routev3.Route_Route{
							Route: &routev3.RouteAction{
								ClusterSpecifier: &routev3.RouteAction_Cluster{
									Cluster: service,
								},
								Timeout: durationpb.New(0),
							},
						},
					},
				},
			},
		},
	}
}

func buildRouteFromSpec(spec RouteConfigSpec) (*routev3.RouteConfiguration, error) {
	vhost := &routev3.VirtualHost{
		Name:    "backend",
		Domains: spec.Domains,
		Routes:  make([]*routev3.Route, 0, len(spec.Rules)),
	}
	if len(vhost.Domains) == 0 {
		vhost.Domains = []string{"*"}
	}

	for _, rule := range spec.Rules {
		if _, ok := backends[rule.Cluster]; !ok {
			return nil, fmt.Errorf("route %q references missing cluster %q", spec.RouteName, rule.Cluster)
		}

		routeAction := &routev3.RouteAction{
			ClusterSpecifier: &routev3.RouteAction_Cluster{
				Cluster: rule.Cluster,
			},
			Timeout: durationpb.New(time.Duration(rule.TimeoutMS) * time.Millisecond),
		}
		if rule.TimeoutMS == 0 {
			routeAction.Timeout = durationpb.New(0)
		}
		if rule.PrefixRewrite != "" {
			routeAction.PrefixRewrite = rule.PrefixRewrite
		}
		if rule.HostRewriteLiteral != "" {
			routeAction.HostRewriteSpecifier = &routev3.RouteAction_HostRewriteLiteral{
				HostRewriteLiteral: rule.HostRewriteLiteral,
			}
		}
		if rule.RetryPolicy != nil {
			routeAction.RetryPolicy = buildRetryPolicy(*rule.RetryPolicy)
		}

		vhost.Routes = append(vhost.Routes, &routev3.Route{
			Match: &routev3.RouteMatch{
				PathSpecifier: &routev3.RouteMatch_Prefix{
					Prefix: rule.Prefix,
				},
			},
			Action: &routev3.Route_Route{
				Route: routeAction,
			},
		})
	}

	return &routev3.RouteConfiguration{
		Name:         spec.RouteName,
		VirtualHosts: []*routev3.VirtualHost{vhost},
	}, nil
}

func buildRetryPolicy(spec RetryPolicySpec) *routev3.RetryPolicy {
	retryOn := spec.RetryOn
	if retryOn == "" {
		retryOn = "5xx,connect-failure,refused-stream,reset"
	}
	numRetries := spec.NumRetries
	if numRetries == 0 {
		numRetries = 3
	}

	policy := &routev3.RetryPolicy{
		RetryOn:    retryOn,
		NumRetries: &wrapperspb.UInt32Value{Value: numRetries},
	}
	if spec.PerTryTimeoutMS > 0 {
		policy.PerTryTimeout = durationpb.New(time.Duration(spec.PerTryTimeoutMS) * time.Millisecond)
	}
	if len(spec.RetriableStatusCodes) > 0 {
		policy.RetriableStatusCodes = spec.RetriableStatusCodes
	}
	if spec.HostSelectionRetryMaxAttempts > 0 {
		policy.HostSelectionRetryMaxAttempts = int64(spec.HostSelectionRetryMaxAttempts)
	}
	return policy
}

func buildListener(spec ListenerSpec) (*listenerv3.Listener, error) {
	routerConfig, err := anypb.New(&router.Router{})
	if err != nil {
		return nil, fmt.Errorf("pack router filter: %w", err)
	}

	manager := &hcm.HttpConnectionManager{
		StatPrefix: spec.Name,
		RouteSpecifier: &hcm.HttpConnectionManager_Rds{
			Rds: &hcm.Rds{
				RouteConfigName: spec.RouteName,
				ConfigSource: &core.ConfigSource{
					ResourceApiVersion: resource.DefaultAPIVersion,
					ConfigSourceSpecifier: &core.ConfigSource_Ads{
						Ads: &core.AggregatedConfigSource{},
					},
				},
			},
		},
		HttpFilters: []*hcm.HttpFilter{
			{
				Name: "envoy.filters.http.router",
				ConfigType: &hcm.HttpFilter_TypedConfig{
					TypedConfig: routerConfig,
				},
			},
		},
	}

	typedConfig, err := anypb.New(manager)
	if err != nil {
		return nil, fmt.Errorf("pack http connection manager: %w", err)
	}

	filterChain := &listenerv3.FilterChain{
		Filters: []*listenerv3.Filter{
			{
				Name: "envoy.filters.network.http_connection_manager",
				ConfigType: &listenerv3.Filter_TypedConfig{
					TypedConfig: typedConfig,
				},
			},
		},
	}

	if spec.TLS != nil && spec.TLS.Enabled {
		transportSocket, err := buildDownstreamTLSTransportSocket(*spec.TLS)
		if err != nil {
			return nil, err
		}
		filterChain.TransportSocket = transportSocket
	}

	return &listenerv3.Listener{
		Name:         spec.Name,
		Address:      socketAddress(spec.Address, spec.Port),
		FilterChains: []*listenerv3.FilterChain{filterChain},
	}, nil
}

func buildDownstreamTLSTransportSocket(cfg ListenerTLS) (*core.TransportSocket, error) {
	normalizeListenerTLS(&cfg)

	common := &tlsv3.CommonTlsContext{
		AlpnProtocols: cfg.ALPNProtocols,
		TlsCertificateSdsSecretConfigs: []*tlsv3.SdsSecretConfig{
			{
				Name:      cfg.SecretName,
				SdsConfig: buildPathConfigSource(cfg.SDSPath, cfg.WatchedDirectory),
			},
		},
	}

	if cfg.RequireClientCert {
		common.ValidationContextType = &tlsv3.CommonTlsContext_ValidationContextSdsSecretConfig{
			ValidationContextSdsSecretConfig: &tlsv3.SdsSecretConfig{
				Name: cfg.ValidationSecretName,
				SdsConfig: buildPathConfigSource(
					cfg.ValidationSDSPath,
					watchedDir(cfg.ValidationSDSPath, cfg.WatchedDirectory),
				),
			},
		}
	}

	downstream := &tlsv3.DownstreamTlsContext{
		CommonTlsContext: common,
	}
	if cfg.RequireClientCert {
		downstream.RequireClientCertificate = &wrapperspb.BoolValue{Value: true}
	}

	typedConfig, err := anypb.New(downstream)
	if err != nil {
		return nil, fmt.Errorf("pack downstream tls context: %w", err)
	}

	return &core.TransportSocket{
		Name: "envoy.transport_sockets.tls",
		ConfigType: &core.TransportSocket_TypedConfig{
			TypedConfig: typedConfig,
		},
	}, nil
}

func buildUpstreamTLSTransportSocket(cfg UpstreamTLS) (*core.TransportSocket, error) {
	common := &tlsv3.CommonTlsContext{}

	if cfg.TrustedCAFile != "" {
		common.ValidationContextType = &tlsv3.CommonTlsContext_ValidationContext{
			ValidationContext: &tlsv3.CertificateValidationContext{
				TrustedCa: &core.DataSource{
					Specifier: &core.DataSource_Filename{
						Filename: cfg.TrustedCAFile,
					},
				},
			},
		}
	}
	if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		common.TlsCertificates = []*tlsv3.TlsCertificate{
			{
				CertificateChain: &core.DataSource{
					Specifier: &core.DataSource_Filename{Filename: cfg.ClientCertFile},
				},
				PrivateKey: &core.DataSource{
					Specifier: &core.DataSource_Filename{Filename: cfg.ClientKeyFile},
				},
			},
		}
	}

	upstream := &tlsv3.UpstreamTlsContext{
		CommonTlsContext:     common,
		Sni:                  cfg.SNI,
		AutoHostSni:          cfg.AutoHostSNI,
		AutoSniSanValidation: cfg.AutoSNISANValidation,
	}

	typedConfig, err := anypb.New(upstream)
	if err != nil {
		return nil, fmt.Errorf("pack upstream tls context: %w", err)
	}

	return &core.TransportSocket{
		Name: "envoy.transport_sockets.tls",
		ConfigType: &core.TransportSocket_TypedConfig{
			TypedConfig: typedConfig,
		},
	}, nil
}

func buildPathConfigSource(path, watched string) *core.ConfigSource {
	return &core.ConfigSource{
		ResourceApiVersion: resource.DefaultAPIVersion,
		ConfigSourceSpecifier: &core.ConfigSource_PathConfigSource{
			PathConfigSource: &core.PathConfigSource{
				Path: path,
				WatchedDirectory: &core.WatchedDirectory{
					Path: watched,
				},
			},
		},
	}
}

func watchedDir(path, fallback string) string {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return fallback
	}
	return dir
}

func socketAddress(address string, port uint32) *core.Address {
	return &core.Address{
		Address: &core.Address_SocketAddress{
			SocketAddress: &core.SocketAddress{
				Address: address,
				PortSpecifier: &core.SocketAddress_PortValue{
					PortValue: port,
				},
			},
		},
	}
}

func mapRoutingPriority(priority string) core.RoutingPriority {
	switch strings.ToUpper(strings.TrimSpace(priority)) {
	case "HIGH":
		return core.RoutingPriority_HIGH
	default:
		return core.RoutingPriority_DEFAULT
	}
}

func mapLBPolicy(policy string) cluster.Cluster_LbPolicy {
	switch strings.ToUpper(strings.TrimSpace(policy)) {
	case "LEAST_REQUEST":
		return cluster.Cluster_LEAST_REQUEST
	case "RANDOM":
		return cluster.Cluster_RANDOM
	case "RING_HASH":
		return cluster.Cluster_RING_HASH
	case "MAGLEV":
		return cluster.Cluster_MAGLEV
	default:
		return cluster.Cluster_ROUND_ROBIN
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func badRequest(w http.ResponseWriter, msg string) {
	writeJSON(w, http.StatusBadRequest, APIResponse{Message: msg})
}

func internalError(w http.ResponseWriter, msg string) {
	writeJSON(w, http.StatusInternalServerError, APIResponse{Message: msg})
}

func methodNotAllowed(w http.ResponseWriter) {
	writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Message: "method not allowed"})
}
