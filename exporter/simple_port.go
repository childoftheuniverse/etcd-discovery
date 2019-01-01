/*
Package exporter provides exported named etcd ports.
This binds to an anonymous port, exports the host:port pair through etcd
and returns the port to the caller.

There are convenience methods for exporting a TLS port and an HTTP service.
*/
package exporter

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strconv"

	"github.com/childoftheuniverse/etcd-discovery"
	etcd "go.etcd.io/etcd/clientv3"
	"github.com/golang/protobuf/proto"
)

/*
ServiceExporter exists because we need to initialize our etcd client
beforehand and keep it somewhere.
*/
type ServiceExporter struct {
	kv                 etcd.KV
	lease              etcd.Lease
	path               string
	leaseID            etcd.LeaseID
	keepaliveResponses <-chan *etcd.LeaseKeepAliveResponse
}

func consumeKeepaliveResponses(ch <-chan *etcd.LeaseKeepAliveResponse) {
	for _ = range ch {
	}
}

/*
NewExporter creates a new exporter object which can later be used to create
exported ports and services. This will create a client connection to etcd.
If the connection is severed, once the etcd lease is going to expire the
port will stop being exported.
The specified ttl (which must be at least 5 (seconds)) determines how
frequently the lease will be renewed.
*/
func NewExporter(ctx context.Context, etcdURL string, ttl int64) (
	*ServiceExporter, error) {
	var self *ServiceExporter
	var client *etcd.Client
	var err error

	if client, err = etcd.NewFromURL(etcdURL); err != nil {
		return nil, err
	}

	self = &ServiceExporter{
		kv:    client,
		lease: client,
	}

	return self, self.initLease(ctx, ttl)
}

/*
NewExporterFromConfig creates a new exporter by reading etcd flags from
the specified configuration. This will create a client connection to
etcd. If the connection is severed, once the etcd lease is going to expire the
port will stop being exported.

The specified ttl (which must be at least 5 (seconds)) determines how
frequently the lease will be renewed.
*/
func NewExporterFromConfig(ctx context.Context, config etcd.Config, ttl int64) (
	*ServiceExporter, error) {
	var self *ServiceExporter
	var client *etcd.Client
	var err error

	if client, err = etcd.New(config); err != nil {
		return nil, err
	}

	self = &ServiceExporter{
		kv:    client,
		lease: client,
	}

	return self, self.initLease(ctx, ttl)
}

/*
NewExporterFromClient creates a new exporter by reading etcd flags from the
specified configuration file.
*/
func NewExporterFromClient(
	ctx context.Context, kv etcd.KV, lease etcd.Lease, ttl int64) (
	*ServiceExporter, error) {
	var rv = &ServiceExporter{
		kv:    kv,
		lease: lease,
	}

	return rv, rv.initLease(ctx, ttl)
}

/*
initLease initializes the lease on the etcd service which will be used to export
ports in the future.
*/
func (e *ServiceExporter) initLease(ctx context.Context, ttl int64) error {
	var lease *etcd.LeaseGrantResponse
	var err error

	if lease, err = e.lease.Grant(ctx, ttl); err != nil {
		return err
	}

	e.keepaliveResponses, err = e.lease.KeepAlive(
		context.Background(), lease.ID)
	e.leaseID = lease.ID

	if err != nil {
		log.Print("Error establishing keepalive: ", err)
	}

	go consumeKeepaliveResponses(e.keepaliveResponses)

	return nil
}

/*
NewExportedPort opens a new anonymous port on "ip" and export it through etcd
as "servicename". If "ip" is not a host:port pair, the port will be chosen at
random.
*/
func (e *ServiceExporter) NewExportedPort(
	ctx context.Context, network, ip, service string) (net.Listener, error) {
	var record discovery.ExportedServiceRecord
	var recordData []byte
	var path string
	var host, hostport string
	var port int
	var l net.Listener
	var err error

	if _, _, err = net.SplitHostPort(ip); err != nil {
		// Apparently, it's not in host:port format.
		hostport = net.JoinHostPort(ip, "0")
	} else {
		hostport = ip
	}

	if l, err = net.Listen(network, hostport); err != nil {
		return nil, err
	}

	// Use the lease ID as part of the path; it would be reasonable to expect
	// it to be unique.
	path = fmt.Sprintf("/ns/service/%s/%016x", service, e.leaseID)

	if host, hostport, err = net.SplitHostPort(l.Addr().String()); err != nil {
		l.Close()
		return nil, err
	}

	// Fill in discovery protocol buffer.
	record.Protocol = "tcp"
	record.Address = host
	if port, err = strconv.Atoi(hostport); err != nil {
		// Probably a named port. TODO: should be looked up in /etc/services.
		l.Close()
		return nil, err
	}
	record.Port = int32(port)

	if recordData, err = proto.Marshal(&record); err != nil {
		l.Close()
		return nil, err
	}

	// Now write our host:port pair to etcd. Let etcd choose the file name.
	if _, err = e.kv.Put(ctx, path, string(recordData),
		etcd.WithLease(e.leaseID)); err != nil {
		l.Close()
		return nil, err
	}

	e.path = path

	return l, nil
}

/*
NewExportedTLSPort opens a new anonymous port on "ip" and export it through
etcd as "servicename" (see NewExportedPort). Associates the TLS configuration
"config". If "ip" is a host:port pair, the port will be overridden.
*/
func (e *ServiceExporter) NewExportedTLSPort(
	ctx context.Context, network, ip, servicename string,
	config *tls.Config) (net.Listener, error) {
	var l net.Listener
	var err error

	// We can just create a new port as above...
	if l, err = e.NewExportedPort(ctx, network, ip, servicename); err != nil {
		return nil, err
	}

	// ... and inject a TLS context.
	return tls.NewListener(l, config), nil
}

/*
UnexportPort removes the associated exported port. This will only delete the
most recently exported port. Exported ports will disappear by themselves once
the process dies, but this will expedite the process.
*/
func (e *ServiceExporter) UnexportPort(ctx context.Context) error {
	var err error

	if len(e.path) == 0 {
		return nil
	}

	if _, err = e.kv.Delete(ctx, e.path); err != nil {
		return err
	}

	return nil
}
