/*
http://www.apache.org/licenses/LICENSE-2.0.txt


Copyright 2016 Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package plugin

import (
	"encoding/json"
	"fmt"
	"net"
	log "github.com/Sirupsen/logrus"

	"github.com/intelsdi-x/snap-plugin-lib-go/v1/plugin/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"crypto/tls"
)

// Plugin is the base plugin type. All plugins must implement GetConfigPolicy.
type Plugin interface {
	GetConfigPolicy() (ConfigPolicy, error)
}

// Collector is a plugin which is the source of new data in the Snap pipeline.
type Collector interface {
	Plugin

	GetMetricTypes(Config) ([]Metric, error)
	CollectMetrics([]Metric) ([]Metric, error)
}

// Processor is a plugin which filters, agregates, or decorates data in the
// Snap pipeline.
type Processor interface {
	Plugin

	Process([]Metric, Config) ([]Metric, error)
}

// Publisher is a sink in the Snap pipeline.  It publishes data into another
// System, completing a Workflow path.
type Publisher interface {
	Plugin

	Publish([]Metric, Config) error
}

// StartCollector is given a Collector implementation and its metadata,
// generates a response for the initial stdin / stdout handshake, and starts
// the plugin's gRPC server.
func StartCollector(plugin Collector, name string, version int, opts ...MetaOpt) int {
	log.WithFields(log.Fields{
		"module": "snap-plugin-lib-go",
		"block": "v1/plugin/plugin.go",
		"function": "StartCollector",
		"plugin_name": name,
		"plugin_version": version,
	}).Info("Debug Iza - starting a collector inside plugin-lib")

	args, _ := getArgs()

	log.WithFields(log.Fields{
		"module": "snap-plugin-lib-go",
		"block": "v1/plugin/plugin.go",
		"function": "StartCollector",
		"args_certPath": args.CertPath,
		"args_keyPath": args.KeyPath,
	}).Info("Debug Iza - starting a collector inside plugin-lib")

	m := newMeta(collectorType, name, version, opts...)
	log.WithFields(log.Fields{
		"module": "snap-plugin-lib-go",
		"block": "v1/plugin/plugin.go",
		"function": "StartCollector",
		"plugin_name": name,
		"plugin_version": version,
	}).Info("Debug Iza - creating a new GRPC server")

	certPath := args.CertPath
	keyPath := args.KeyPath

	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		log.WithFields(log.Fields{
			"_module":     "v1/plugin/plugin.go:90",
			"_block":      "snap-plugin-lib-go",
			"certPath": certPath,
			"keyPath": keyPath,
		}).Errorf("Cannot load grpc key pair, err = %s", err.Error())
		panic(err)
	}

	credential := credentials.NewTLS(&tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{certificate},
	})

	grpcServerOpts := []grpc.ServerOption{
		grpc.Creds(credential),
	}

	server := grpc.NewServer(grpcServerOpts...)
	// TODO(danielscottt) SSL
	proxy := &collectorProxy{
		plugin:      plugin,
		pluginProxy: *newPluginProxy(plugin),
	}
	rpc.RegisterCollectorServer(server, proxy)
	return startPlugin(server, m, &proxy.pluginProxy)
}

// StartProcessor is given a Processor implementation and its metadata,
// generates a response for the initial stdin / stdout handshake, and starts
// the plugin's gRPC server.
func StartProcessor(plugin Processor, name string, version int, opts ...MetaOpt) int {
	getArgs()
	m := newMeta(processorType, name, version, opts...)

	server := grpc.NewServer()
	// TODO(danielscottt) SSL
	proxy := &processorProxy{
		plugin:      plugin,
		pluginProxy: *newPluginProxy(plugin),
	}
	rpc.RegisterProcessorServer(server, proxy)
	return startPlugin(server, m, &proxy.pluginProxy)
}

// StartPublisher is given a Publisher implementation and its metadata,
// generates a response for the initial stdin / stdout handshake, and starts
// the plugin's gRPC server.
func StartPublisher(plugin Publisher, name string, version int, opts ...MetaOpt) int {
	args, _ := getArgs()

	m := newMeta(publisherType, name, version, opts...)

	certPath := args.CertPath
	keyPath := args.KeyPath

	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		log.WithFields(log.Fields{
			"_module":     "v1/plugin/plugin.go:90",
			"_block":      "snap-plugin-lib-go",
			"certPath": certPath,
			"keyPath": keyPath,
		}).Errorf("Cannot load grpc key pair, err = %s", err.Error())
		panic(err)
	}

	credential := credentials.NewTLS(&tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{certificate},
	})

	grpcServerOpts := []grpc.ServerOption{
		grpc.Creds(credential),
	}

	server := grpc.NewServer(grpcServerOpts...)

	// TODO(danielscottt) SSL
	proxy := &publisherProxy{
		plugin:      plugin,
		pluginProxy: *newPluginProxy(plugin),
	}
	rpc.RegisterPublisherServer(server, proxy)
	return startPlugin(server, m, &proxy.pluginProxy)
}

type server interface {
	Serve(net.Listener) error
}

type preamble struct {
	Meta          meta
	ListenAddress string
	PprofAddress  string
	Type          pluginType
	State         int
	ErrorMessage  string
}

func startPlugin(srv server, m meta, p *pluginProxy) int {
	log.WithFields(log.Fields{
		"module": "snap-plugin-lib-go",
		"block": "v1/plugin/plugin.go",
		"function": "startPlugin",
	}).Info("Debug Iza - starting a plugin, do net.Listen")
	l, err := net.Listen("tcp", ":"+listenPort)
	if err != nil {
		panic("Unable to get open port")
	}
	l.Close()

	addr := fmt.Sprintf("127.0.0.1:%v", l.Addr().(*net.TCPAddr).Port)
	log.WithFields(log.Fields{
		"module": "snap-plugin-lib-go",
		"block": "v1/plugin/plugin.go",
		"function": "startPlugin",
		"addr": addr,
	}).Info("Debug Iza -getting an address to do net.Listen")

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		// TODO(danielscottt): logging
		panic(err)
	}
	go func() {
		err := srv.Serve(lis)
		if err != nil {
			log.WithFields(log.Fields{
				"module": "snap-plugin-lib-go",
				"block": "v1/plugin/plugin.go",
				"function": "startPlugin",
			}).Info("Debug Iza - scannot serve on listener")
			panic(err)
		}
	}()
	resp := preamble{
		Meta:          m,
		ListenAddress: addr,
		Type:          m.Type,
		PprofAddress:  pprofPort,
		State:         0, // Hardcode success since panics on err
	}
	preambleJSON, err := json.Marshal(resp)

	log.WithFields(log.Fields{
		"module": "snap-plugin-lib-go",
		"block": "v1/plugin/plugin.go",
		"function": "startPlugin",
		"meta_unsecure": m.Unsecure,
		"ListenAddress": addr,
	}).Info("Debug Iza - creating a preamble")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(preambleJSON))
	go p.HeartbeatWatch()
	// TODO(danielscottt): exit code
	<-p.halt
	return 0
}
