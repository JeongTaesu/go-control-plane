package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
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

	backends  = map[string]Backend{}
	listeners = map[string]ListenerSpec{}
	routesMap = map[string]RouteSpec{}
)

type Backend struct {
	Service       string       `json:"service"`
	Address       string       `json:"address"`
	Port          uint32       `json:"port"`
	DiscoveryType string       `json:"discovery_type"`
	UseHTTP2      bool         `json:"use_http2,omitempty"`
	UpstreamTLS   *UpstreamTLS `json:"upstream_tls,omitempty"`
}

type UpstreamTLS struct {
	Enabled              bool   `json:"enabled"`
	SNI                  string `json:"sni,omitempty"`
	AutoHostSNI          bool   `json:"auto_host_sni,omitempty"`
	AutoSNISANValidation bool   `json:"auto_sni_san_validation,omitempty"`
	TrustedCAFile        string `json:"trusted_ca_file,omitempty"`
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
	Prefix        string `json:"prefix"`
	Cluster       string `json:"cluster"`
	PrefixRewrite string `json:"prefix_rewrite,omitempty"`
}

type RouteSpec struct {
	RouteName string      `json:"route_name"`
	Domains   []string    `json:"domains,omitempty"`
	Rules     []RouteRule `json:"rules"`
}

type BackendUpsertRequest struct {
	Service       string       `json:"service"`
	Address       string       `json:"address,omitempty"`
	IP            string       `json:"ip,omitempty"`
	Port          uint32       `json:"port"`
	DiscoveryType string       `json:"discovery_type,omitempty"`
	UseHTTP2      bool         `json:"use_http2,omitempty"`
	UpstreamTLS   *UpstreamTLS `json:"upstream_tls,omitempty"`
}

type ListenerUpsertRequest struct {
	Name      string       `json:"name"`
	Address   string       `json:"address"`
	Port      uint32       `json:"port"`
	RouteName string       `json:"route_name"`
	Service   string       `json:"service"`
	TLS       *ListenerTLS `json:"tls,omitempty"`
}

type RouteUpsertRequest struct {
	RouteName string      `json:"route_name"`
	Domains   []string    `json:"domains,omitempty"`
	Rules     []RouteRule `json:"rules"`
}

type APIResponse struct {
	Message string `json:"message"`
	Version uint64 `json:"version,omitempty"`
}

type StateResponse struct {
	Version   uint64         `json:"version"`
	Backends  []Backend      `json:"backends"`
	Listeners []ListenerSpec `json:"listeners"`
	Routes    []RouteSpec    `json:"routes"`
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

	if req.Service == "" {
		badRequest(w, "service required")
		return
	}
	if req.Address == "" && req.IP != "" {
		req.Address = req.IP
	}
	if req.Address == "" {
		badRequest(w, "address or ip required")
		return
	}
	if req.Port == 0 {
		badRequest(w, "port required")
		return
	}

	normalizeBackendRequest(&req)
	if err := validateBackendRequest(req); err != nil {
		badRequest(w, err.Error())
		return
	}

	mu.Lock()
	old, hadOld := backends[req.Service]
	backends[req.Service] = Backend{
		Service:       req.Service,
		Address:       req.Address,
		Port:          req.Port,
		DiscoveryType: req.DiscoveryType,
		UseHTTP2:      req.UseHTTP2,
		UpstreamTLS:   req.UpstreamTLS,
	}
	if err := rebuildSnapshotLocked(); err != nil {
		if hadOld {
			backends[req.Service] = old
		} else {
			delete(backends, req.Service)
		}
		mu.Unlock()
		internalError(w, "failed to rebuild snapshot: "+err.Error())
		return
	}
	currentVersion := atomic.LoadUint64(&version)
	created := backends[req.Service]
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"message": "cluster upserted",
		"cluster": created,
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
	if _, ok := backends[service]; !ok {
		mu.Unlock()
		writeJSON(w, http.StatusNotFound, APIResponse{Message: "cluster not found"})
		return
	}

	for _, rt := range routesMap {
		for _, rule := range rt.Rules {
			if rule.Cluster == service {
				mu.Unlock()
				badRequest(w, fmt.Sprintf("cluster %q is in use by route %q", service, rt.RouteName))
				return
			}
		}
	}

	for _, l := range listeners {
		if l.Service == service {
			if _, hasCustomRoute := routesMap[l.RouteName]; !hasCustomRoute {
				mu.Unlock()
				badRequest(w, fmt.Sprintf("cluster %q is used as default backend by listener %q", service, l.Name))
				return
			}
		}
	}

	deleted := backends[service]
	delete(backends, service)

	if err := rebuildSnapshotLocked(); err != nil {
		backends[service] = deleted
		mu.Unlock()
		internalError(w, "failed to rebuild snapshot: "+err.Error())
		return
	}

	currentVersion := atomic.LoadUint64(&version)
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"message": "cluster deleted",
		"cluster": deleted,
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
	if req.Service == "" {
		badRequest(w, "service required")
		return
	}
	if req.RouteName == "" {
		req.RouteName = "route_" + req.Name
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

	old, hadOld := listeners[req.Name]
	listeners[req.Name] = ListenerSpec{
		Name:      req.Name,
		Address:   req.Address,
		Port:      req.Port,
		RouteName: req.RouteName,
		Service:   req.Service,
		TLS:       req.TLS,
	}

	if err := rebuildSnapshotLocked(); err != nil {
		if hadOld {
			listeners[req.Name] = old
		} else {
			delete(listeners, req.Name)
		}
		mu.Unlock()
		internalError(w, "failed to rebuild snapshot: "+err.Error())
		return
	}

	currentVersion := atomic.LoadUint64(&version)
	created := listeners[req.Name]
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"message":  "listener upserted",
		"listener": created,
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

	mu.Lock()
	for i, rule := range req.Rules {
		if rule.Prefix == "" {
			mu.Unlock()
			badRequest(w, fmt.Sprintf("rules[%d].prefix required", i))
			return
		}
		if rule.Cluster == "" {
			mu.Unlock()
			badRequest(w, fmt.Sprintf("rules[%d].cluster required", i))
			return
		}
		if _, ok := backends[rule.Cluster]; !ok {
			mu.Unlock()
			badRequest(w, fmt.Sprintf("rules[%d].cluster %q not found", i, rule.Cluster))
			return
		}
	}

	old, hadOld := routesMap[req.RouteName]
	routesMap[req.RouteName] = RouteSpec{
		RouteName: req.RouteName,
		Domains:   req.Domains,
		Rules:     req.Rules,
	}

	if err := rebuildSnapshotLocked(); err != nil {
		if hadOld {
			routesMap[req.RouteName] = old
		} else {
			delete(routesMap, req.RouteName)
		}
		mu.Unlock()
		internalError(w, "failed to rebuild snapshot: "+err.Error())
		return
	}

	currentVersion := atomic.LoadUint64(&version)
	created := routesMap[req.RouteName]
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"message": "route upserted",
		"route":   created,
		"version": currentVersion,
	})
}

func routeDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		methodNotAllowed(w)
		return
	}

	routeName := r.URL.Query().Get("route_name")
	if routeName == "" {
		badRequest(w, "route_name query parameter required")
		return
	}

	mu.Lock()
	rt, ok := routesMap[routeName]
	if !ok {
		mu.Unlock()
		writeJSON(w, http.StatusNotFound, APIResponse{Message: "route not found"})
		return
	}

	delete(routesMap, routeName)
	if err := rebuildSnapshotLocked(); err != nil {
		routesMap[routeName] = rt
		mu.Unlock()
		internalError(w, "failed to rebuild snapshot: "+err.Error())
		return
	}

	currentVersion := atomic.LoadUint64(&version)
	mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"message": "route deleted",
		"route":   rt,
		"version": currentVersion,
	})
}

func routesListHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}

	mu.Lock()
	items := make([]RouteSpec, 0, len(routesMap))
	for _, rt := range routesMap {
		items = append(items, rt)
	}
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
		Version:   atomic.LoadUint64(&version),
		Backends:  make([]Backend, 0, len(backends)),
		Listeners: make([]ListenerSpec, 0, len(listeners)),
		Routes:    make([]RouteSpec, 0, len(routesMap)),
	}
	for _, b := range backends {
		resp.Backends = append(resp.Backends, b)
	}
	for _, l := range listeners {
		resp.Listeners = append(resp.Listeners, l)
	}
	for _, rt := range routesMap {
		resp.Routes = append(resp.Routes, rt)
	}
	mu.Unlock()

	writeJSON(w, http.StatusOK, resp)
}

func rebuildSnapshot() error {
	mu.Lock()
	defer mu.Unlock()
	return rebuildSnapshotLocked()
}

func rebuildSnapshotLocked() error {
	clusterResources := make([]types.Resource, 0, len(backends))
	endpointResources := make([]types.Resource, 0, len(backends))
	routeResources := make([]types.Resource, 0, len(listeners)+len(routesMap))
	listenerResources := make([]types.Resource, 0, len(listeners))

	for _, b := range backends {
		clusterResource, endpointResource, err := buildClusterAndEndpoint(b)
		if err != nil {
			return fmt.Errorf("build cluster %q: %w", b.Service, err)
		}
		clusterResources = append(clusterResources, clusterResource)
		if endpointResource != nil {
			endpointResources = append(endpointResources, endpointResource)
		}
	}

	seenRoutes := map[string]bool{}
	for _, l := range listeners {
		if !seenRoutes[l.RouteName] {
			routeResources = append(routeResources, buildRouteForListener(l))
			seenRoutes[l.RouteName] = true
		}

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

func buildClusterAndEndpoint(b Backend) (*cluster.Cluster, *endpointv3.ClusterLoadAssignment, error) {
	clusterResource := &cluster.Cluster{
		Name:           b.Service,
		ConnectTimeout: durationpb.New(5 * time.Second),
		LbPolicy:       cluster.Cluster_ROUND_ROBIN,
	}

	if b.UseHTTP2 {
		clusterResource.Http2ProtocolOptions = &core.Http2ProtocolOptions{}
	}

	if b.UpstreamTLS != nil && b.UpstreamTLS.Enabled {
		transportSocket, err := buildUpstreamTLSTransportSocket(*b.UpstreamTLS)
		if err != nil {
			return nil, nil, err
		}
		clusterResource.TransportSocket = transportSocket
	}

	switch b.DiscoveryType {
	case "EDS":
		clusterResource.ClusterDiscoveryType = &cluster.Cluster_Type{
			Type: cluster.Cluster_EDS,
		}
		clusterResource.EdsClusterConfig = &cluster.Cluster_EdsClusterConfig{
			EdsConfig: &core.ConfigSource{
				ResourceApiVersion: resource.DefaultAPIVersion,
				ConfigSourceSpecifier: &core.ConfigSource_Ads{
					Ads: &core.AggregatedConfigSource{},
				},
			},
		}
		return clusterResource, buildEndpoint(b.Service, b.Address, b.Port), nil

	case "LOGICAL_DNS":
		clusterResource.ClusterDiscoveryType = &cluster.Cluster_Type{
			Type: cluster.Cluster_LOGICAL_DNS,
		}
		clusterResource.LoadAssignment = buildEndpoint(b.Service, b.Address, b.Port)
		return clusterResource, nil, nil

	default:
		return nil, nil, fmt.Errorf("unsupported discovery_type %q", b.DiscoveryType)
	}
}

func buildUpstreamTLSTransportSocket(cfg UpstreamTLS) (*core.TransportSocket, error) {
	upstream := &tlsv3.UpstreamTlsContext{
		CommonTlsContext: &tlsv3.CommonTlsContext{},
	}

	if cfg.SNI != "" {
		upstream.Sni = cfg.SNI
	}
	upstream.AutoHostSni = cfg.AutoHostSNI
	upstream.AutoSniSanValidation = cfg.AutoSNISANValidation

	if cfg.TrustedCAFile != "" {
		upstream.CommonTlsContext.ValidationContextType = &tlsv3.CommonTlsContext_ValidationContext{
			ValidationContext: &tlsv3.CertificateValidationContext{
				TrustedCa: &core.DataSource{
					Specifier: &core.DataSource_Filename{
						Filename: cfg.TrustedCAFile,
					},
				},
			},
		}
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

func buildEndpoint(service, address string, port uint32) *endpointv3.ClusterLoadAssignment {
	return &endpointv3.ClusterLoadAssignment{
		ClusterName: service,
		Endpoints: []*endpointv3.LocalityLbEndpoints{
			{
				LbEndpoints: []*endpointv3.LbEndpoint{
					{
						HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
							Endpoint: &endpointv3.Endpoint{
								Address: &core.Address{
									Address: &core.Address_SocketAddress{
										SocketAddress: &core.SocketAddress{
											Address: address,
											PortSpecifier: &core.SocketAddress_PortValue{
												PortValue: port,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func buildRouteForListener(listener ListenerSpec) *routev3.RouteConfiguration {
	if custom, ok := routesMap[listener.RouteName]; ok {
		return buildRouteFromSpec(custom)
	}

	return &routev3.RouteConfiguration{
		Name: listener.RouteName,
		VirtualHosts: []*routev3.VirtualHost{
			{
				Name:    "backend",
				Domains: []string{"*"},
				Routes: []*routev3.Route{
					{
						Match: &routev3.RouteMatch{
							PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"},
						},
						Action: &routev3.Route_Route{
							Route: &routev3.RouteAction{
								ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: listener.Service},
								Timeout:          durationpb.New(0),
							},
						},
					},
				},
			},
		},
	}
}

func buildRouteFromSpec(spec RouteSpec) *routev3.RouteConfiguration {
	domains := spec.Domains
	if len(domains) == 0 {
		domains = []string{"*"}
	}

	routes := make([]*routev3.Route, 0, len(spec.Rules))
	for _, rule := range spec.Rules {
		action := &routev3.RouteAction{
			ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: rule.Cluster},
			Timeout:          durationpb.New(0),
		}
		if rule.PrefixRewrite != "" {
			action.PrefixRewrite = rule.PrefixRewrite
		}

		routes = append(routes, &routev3.Route{
			Match: &routev3.RouteMatch{
				PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: rule.Prefix},
			},
			Action: &routev3.Route_Route{Route: action},
		})
	}

	return &routev3.RouteConfiguration{
		Name: spec.RouteName,
		VirtualHosts: []*routev3.VirtualHost{
			{
				Name:    "backend",
				Domains: domains,
				Routes:  routes,
			},
		},
	}
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
				Name:       "envoy.filters.http.router",
				ConfigType: &hcm.HttpFilter_TypedConfig{TypedConfig: routerConfig},
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
				Name:       "envoy.filters.network.http_connection_manager",
				ConfigType: &listenerv3.Filter_TypedConfig{TypedConfig: typedConfig},
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
		Name: spec.Name,
		Address: &core.Address{
			Address: &core.Address_SocketAddress{
				SocketAddress: &core.SocketAddress{
					Address:       spec.Address,
					PortSpecifier: &core.SocketAddress_PortValue{PortValue: spec.Port},
				},
			},
		},
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

	downstream := &tlsv3.DownstreamTlsContext{CommonTlsContext: common}
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

func buildPathConfigSource(path, watched string) *core.ConfigSource {
	return &core.ConfigSource{
		ResourceApiVersion: resource.DefaultAPIVersion,
		ConfigSourceSpecifier: &core.ConfigSource_PathConfigSource{
			PathConfigSource: &core.PathConfigSource{
				Path:             path,
				WatchedDirectory: &core.WatchedDirectory{Path: watched},
			},
		},
	}
}

func normalizeBackendRequest(req *BackendUpsertRequest) {
	req.DiscoveryType = strings.ToUpper(strings.TrimSpace(req.DiscoveryType))
	if req.DiscoveryType == "" {
		if net.ParseIP(req.Address) != nil {
			req.DiscoveryType = "EDS"
		} else {
			req.DiscoveryType = "LOGICAL_DNS"
		}
	}

	if req.UpstreamTLS != nil && req.UpstreamTLS.Enabled {
		if req.UpstreamTLS.SNI == "" && net.ParseIP(req.Address) == nil {
			req.UpstreamTLS.SNI = req.Address
		}
		if net.ParseIP(req.Address) == nil {
			req.UpstreamTLS.AutoHostSNI = true
		}
	}
}

func validateBackendRequest(req BackendUpsertRequest) error {
	switch req.DiscoveryType {
	case "EDS":
		if net.ParseIP(req.Address) == nil {
			return fmt.Errorf("EDS clusters require an IP address, got %q", req.Address)
		}
	case "LOGICAL_DNS":
		// hostname or IP both work here
	default:
		return fmt.Errorf("discovery_type must be EDS or LOGICAL_DNS")
	}
	return nil
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

func watchedDir(path, fallback string) string {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return fallback
	}
	return dir
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
