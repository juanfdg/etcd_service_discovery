package discovery

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.etcd.io/etcd/client"
	"golang.org/x/net/context"
)

const (
	// TTL is a time to live
	// for record in etcd
	TTL = 30 * time.Second

	// KeepAlivePeriod is period of
	// goroutine to
	// refresh the record in etcd.
	KeepAlivePeriod = 20 * time.Second
)


// EtcdRegistrySeviceServerConfig is configuration structure
// for connectiong to etcd instance and identify the service.
type EtcdRegistryServiceServerConfig struct {
	// EtcdEndpoints that service registry client connects to.
	EtcdEndpoints []string

	// ServiceName is the the name of service in application.
	ServiceName string

	// InstanceName is the identification for service instance.
	InstanceName string

	// BaseURL is the url that the service is acessible on.
	BaseURL string

	// ServerLoad is the amount of requests the service node is currently handling
	ServerLoad int

	etcdKey         string
	keepAliveTicker *time.Ticker
	cancel          context.CancelFunc
}

// EtcdRegistryServiceServer  structure implements the basic functionality for service registration in etcd.
// After the Reguster method is called, the client periodically refreshes the record about the service.
type EtcdRegistryServiceServer struct {
	EtcdRegistryServiceServerConfig
	etcdKApi client.KeysAPI
}

// New creates the EtcdRegistrySeviceServer
// with service paramters defined by config.
func New(config EtcdRegistryServiceServerConfig) (*EtcdRegistryServiceServer, error) {
	cfg := client.Config{
		Endpoints:               config.EtcdEndpoints,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)

	if err != nil {
		return nil, err
	}

	etcdClient := &EtcdRegistryServiceServer{
		config,
		client.NewKeysAPI(c),
	}
	return etcdClient, nil
}

// Register register service to configured etcd instance. Once the Register is called, the client
// also periodically calls the refresh goroutine.
func (e *EtcdRegistryServiceServer) Register() error {
	e.etcdKey = buildKey(e.ServiceName, e.InstanceName)
	value := registerDTO{
		e.BaseURL,
		e.ServerLoad,
	}

	val, _ := json.Marshal(value)
	e.keepAliveTicker = time.NewTicker(KeepAlivePeriod)
	ctx, c := context.WithCancel(context.TODO())
	e.cancel = c

	insertFunc := func() error {
		_, err := e.etcdKApi.Set(context.Background(), e.etcdKey, string(val), &client.SetOptions{
			TTL: TTL,
		})
		return err
	}
	err := insertFunc()
	if err != nil {
		return err
	}

	// Exec the keep alive goroutine
	go func() {
		for {
			select {
			case <-e.keepAliveTicker.C:
				insertFunc()
				log.Printf("Keep alive routine for %s", e.ServiceName)
			case <-ctx.Done():
				log.Printf("Shutdown keep alive routine for %s", e.ServiceName)
				return
			}
		}
	}()
	return nil
}

// Unregister gently removes the service instance from etcd. Once the Unregister method is called,
// the periodicall refresh goroutine is cancelled.
func (e *EtcdRegistryServiceServer) Unregister() error {
	e.cancel()
	e.keepAliveTicker.Stop()
	_, err := e.etcdKApi.Delete(context.Background(), e.etcdKey, nil)
	return err
}

// ServicesByName query the etcd instance for service nodes for service
// by given name.
func (e *EtcdRegistryServiceServer) ServicesByName(name string) ([]string, error) {
	response, err := e.etcdKApi.Get(context.Background(), fmt.Sprintf("/%s", name), nil)
	ipList := make([]string, 0)
	if err == nil {
		for _, node := range response.Node.Nodes {
			val := &registerDTO{}
			json.Unmarshal([]byte(node.Value), val)
			ipList = append(ipList, val.BaseURL)
		}
	}
	return ipList, err
}

type registerDTO struct {
	BaseURL string
	ServerLoad int
}

func buildKey(servicetype, instanceName string) string {
	return fmt.Sprintf("%s/%s", servicetype, instanceName)
}


// EtcdRegistrySeviceServerConfig is configuration structure for connectiong to etcd
// instance and identify the service.
type EtcdRegistryServiceClientConfig struct {
	// EtcdEndpoints that service registry client connects to.
	EtcdEndpoints []string

	// ServiceName is the name of service in application.
	ServiceName string
}

// EtcdRegistryServiceServer'  structure implements the
// basic functionality for service registration
// in etcd.
// After the Reguster method is called, the client
// periodically refreshes the record about the
// service.
type EtcdRegistryServiceClient struct {
	EtcdRegistryServiceClientConfig
	etcdKApi client.KeysAPI
}

// New creates the EtcdRegistrySeviceServer
// with service paramters defined by config.
func NewServiceClient(config EtcdRegistryServiceClientConfig) (*EtcdRegistryServiceClient, error) {
	cfg := client.Config{
		Endpoints:               config.EtcdEndpoints,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)

	if err != nil {
		return nil, err
	}

	etcdClient := &EtcdRegistryServiceClient{
		config,
		client.NewKeysAPI(c),
	}
	return etcdClient, nil
}

// Query the etcd instance for service nodes by given service name.
func (e *EtcdRegistryServiceClient) ServicesByName(name string) ([]string, error) {
	response, err := e.etcdKApi.Get(context.Background(), fmt.Sprintf("/%s", name), nil)
	ipList := make([]string, 0)
	if err == nil {
		for _, node := range response.Node.Nodes {
			val := &registerDTO{}
			json.Unmarshal([]byte(node.Value), val)
			ipList = append(ipList, val.BaseURL)
		}
	}
	return ipList, err
}
