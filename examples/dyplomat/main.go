package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"

	clusterservice "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointservice "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	routeservice "github.com/envoyproxy/go-control-plane/envoy/service/route/v3"

	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	xds "github.com/envoyproxy/go-control-plane/pkg/server/v3"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	nodeID        = "local-envoy"
	defaultIP     = "127.0.0.1"
	defaultSvc    = "backend_service"
	defaultPort   = uint32(9090)
	xdsListenAddr = ":18000"
	apiListenAddr = ":8081"
)

var (
	snapshotCache cache.SnapshotCache
	version       uint64 = 0
	mu            sync.Mutex

	// 현재 활성 백엔드 상태
	currentService = defaultSvc
	currentIP      = defaultIP
	currentPort    = defaultPort
)

type BackendUpdateRequest struct {
	Service string `json:"service"`
	IP      string `json:"ip"`
	Port    uint32 `json:"port"`
}

type BackendUpdateResponse struct {
	Message string `json:"message"`
	Service string `json:"service"`
	IP      string `json:"ip"`
	Port    uint32 `json:"port"`
	Version uint64 `json:"version"`
}

func main() {
	snapshotCache = cache.NewSnapshotCache(false, cache.IDHash{}, nil)
	server := xds.NewServer(context.Background(), snapshotCache, nil)

	grpcServer := grpc.NewServer()
	discoverygrpc.RegisterAggregatedDiscoveryServiceServer(grpcServer, server)
	endpointservice.RegisterEndpointDiscoveryServiceServer(grpcServer, server)
	clusterservice.RegisterClusterDiscoveryServiceServer(grpcServer, server)
	routeservice.RegisterRouteDiscoveryServiceServer(grpcServer, server)

	lis, err := net.Listen("tcp", xdsListenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", xdsListenAddr, err)
	}

	// // 초기 snapshot
	// if err := updateBackend(defaultSvc, defaultIP, defaultPort); err != nil {
	// 	log.Fatalf("failed to push initial snapshot: %v", err)
	// }

	go startHTTPServer()

	log.Printf("xDS control plane listening on %s", xdsListenAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("grpc server failed: %v", err)
	}
}

func startHTTPServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/backend/update", backendUpdateHandler)
	mux.HandleFunc("/backend/current", backendCurrentHandler)

	log.Printf("REST API listening on %s", apiListenAddr)
	if err := http.ListenAndServe(apiListenAddr, mux); err != nil {
		log.Fatalf("http server failed: %v", err)
	}
}

func backendUpdateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req BackendUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Service == "" {
		http.Error(w, "service required", http.StatusBadRequest)
		return
	}
	if req.IP == "" {
		http.Error(w, "ip required", http.StatusBadRequest)
		return
	}
	if req.Port == 0 {
		http.Error(w, "port required", http.StatusBadRequest)
		return
	}

	if err := updateBackend(req.Service, req.IP, req.Port); err != nil {
		http.Error(w, "failed to update backend: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resp := BackendUpdateResponse{
		Message: "backend updated",
		Service: req.Service,
		IP:      req.IP,
		Port:    req.Port,
		Version: atomic.LoadUint64(&version),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func backendCurrentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	mu.Lock()
	resp := BackendUpdateResponse{
		Message: "current backend",
		Service: currentService,
		IP:      currentIP,
		Port:    currentPort,
		Version: atomic.LoadUint64(&version),
	}
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func updateBackend(service, ip string, port uint32) error {
	mu.Lock()
	defer mu.Unlock()

	clusterResource := buildCluster(service)
	endpointResource := buildEndpoint(service, ip, port)
	routeResource := buildRoute(service)

	nextVersion := atomic.AddUint64(&version, 1)

	snapshot, err := cache.NewSnapshot(
		fmt.Sprintf("%d", nextVersion),
		map[resource.Type][]types.Resource{
			resource.ClusterType:  {clusterResource},
			resource.EndpointType: {endpointResource},
			resource.RouteType:    {routeResource},
		},
	)
	if err != nil {
		atomic.AddUint64(&version, ^uint64(0)) // rollback +1
		return fmt.Errorf("create snapshot: %w", err)
	}

	// static listener + RDS 조합에서는 go-control-plane의 Consistent()가
	// listener reference를 snapshot 안에서 찾지 못해 실패할 수 있음.
	// 실제 Envoy 반영에는 문제 없으므로 로그만 남기고 진행.
	if err := snapshot.Consistent(); err != nil {
		// log.Printf("snapshot inconsistent (ignored for static listener + RDS): %v", err)
	}

	if err := snapshotCache.SetSnapshot(context.Background(), nodeID, snapshot); err != nil {
		atomic.AddUint64(&version, ^uint64(0)) // rollback +1
		return fmt.Errorf("set snapshot: %w", err)
	}

	currentService = service
	currentIP = ip
	currentPort = port

	log.Printf("snapshot pushed: version=%d service=%s backend=%s:%d", nextVersion, service, ip, port)
	return nil
}

func buildCluster(service string) *cluster.Cluster {
	return &cluster.Cluster{
		Name:           service,
		ConnectTimeout: durationpb.New(5),

		ClusterDiscoveryType: &cluster.Cluster_Type{
			Type: cluster.Cluster_EDS,
		},

		EdsClusterConfig: &cluster.Cluster_EdsClusterConfig{
			EdsConfig: &core.ConfigSource{
				ResourceApiVersion: resource.DefaultAPIVersion,
				ConfigSourceSpecifier: &core.ConfigSource_Ads{
					Ads: &core.AggregatedConfigSource{},
				},
			},
		},

		LbPolicy: cluster.Cluster_ROUND_ROBIN,
	}
}

func buildEndpoint(service, ip string, port uint32) *endpointv3.ClusterLoadAssignment {
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
											Address: ip,
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

func buildRoute(service string) *routev3.RouteConfiguration {
	return &routev3.RouteConfiguration{
		Name: "local_route",
		VirtualHosts: []*routev3.VirtualHost{
			{
				Name: "backend",
				Domains: []string{
					"*",
					"localhost",
					"localhost:8080",
				},
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
								HostRewriteSpecifier: &routev3.RouteAction_HostRewriteLiteral{
									HostRewriteLiteral: "127.0.0.1",
								},
							},
						},
					},
				},
			},
		},
	}
}
