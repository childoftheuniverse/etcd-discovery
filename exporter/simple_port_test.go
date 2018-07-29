package exporter

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"testing"

	discovery "github.com/childoftheuniverse/etcd-discovery"
	"github.com/childoftheuniverse/etcd-discovery/testing/mock_etcd"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
)

func TestNewExportedPort(t *testing.T) {
	controller := gomock.NewController(t)
	mockKV := mock_etcd.NewMockKV(controller)
	mockLease := mock_etcd.NewMockLease(controller)
	ctx := context.TODO()

	mockLease.EXPECT().Grant(gomock.Eq(ctx), gomock.Eq(int64(30000))).Return(
		&clientv3.LeaseGrantResponse{
			ID:    23,
			TTL:   30000,
			Error: "",
		}, nil)
	mockLease.EXPECT().KeepAlive(
		gomock.Eq(context.Background()),
		gomock.Eq(clientv3.LeaseID(23))).Return(
		make(chan *clientv3.LeaseKeepAliveResponse), nil)

	exporter, err := NewExporterFromClient(ctx, mockKV, mockLease, 30000)
	if err != nil {
		t.Error("NewExporterFromClient reports error: ", err)
	}
	if exporter == nil {
		t.Fatal("NewExporterFromClient returned nil exporter")
	}

	record := discovery.ExportedServiceRecord{
		Protocol: "tcp",
		Address:  "127.0.0.1",
		Port:     23456,
	}
	bData, err := proto.Marshal(&record)
	if err != nil {
		t.Error("Unable to marshal test ExportedServiceRecord proto")
	}

	mockKV.EXPECT().Put(ctx, "/ns/service/test/0000000000000017", string(bData),
		gomock.Any()).Return(&clientv3.PutResponse{}, nil)

	l, err := exporter.NewExportedPort(ctx, "tcp", "127.0.0.1:23456", "test")
	if err != nil {
		t.Error("NewExportedPort reports error: ", err)
	}
	if l == nil {
		t.Fatal("NewExportedPort returned nil listener")
	}
	defer l.Close()

	mockKV.EXPECT().Delete(ctx, "/ns/service/test/0000000000000017").Return(
		&clientv3.DeleteResponse{}, nil)

	err = exporter.UnexportPort(ctx)
	if err != nil {
		t.Error("UnexportPort reports error: ", err)
	}
}

func TestNewExportedTLSPort(t *testing.T) {
	controller := gomock.NewController(t)
	mockKV := mock_etcd.NewMockKV(controller)
	mockLease := mock_etcd.NewMockLease(controller)
	ctx := context.TODO()

	mockLease.EXPECT().Grant(gomock.Eq(ctx), gomock.Eq(int64(30000))).Return(
		&clientv3.LeaseGrantResponse{
			ID:    23,
			TTL:   30000,
			Error: "",
		}, nil)
	mockLease.EXPECT().KeepAlive(
		gomock.Eq(context.Background()),
		gomock.Eq(clientv3.LeaseID(23))).Return(
		make(chan *clientv3.LeaseKeepAliveResponse), nil)

	exporter, err := NewExporterFromClient(ctx, mockKV, mockLease, 30000)
	if err != nil {
		t.Error("NewExporterFromClient reports error: ", err)
	}
	if exporter == nil {
		t.Fatal("NewExporterFromClient returned nil exporter")
	}

	record := discovery.ExportedServiceRecord{
		Protocol: "tcp",
		Address:  "127.0.0.1",
		Port:     23456,
	}
	bData, err := proto.Marshal(&record)
	if err != nil {
		t.Error("Unable to marshal test ExportedServiceRecord proto")
	}

	mockKV.EXPECT().Put(ctx, "/ns/service/test/0000000000000017", string(bData),
		gomock.Any())

	l, err := exporter.NewExportedTLSPort(
		ctx, "tcp", "127.0.0.1:23456", "test", &tls.Config{})
	if err != nil {
		t.Error("NewExportedPort reports error: ", err)
	}
	if l == nil {
		t.Fatal("NewExportedPort returned nil listener")
	}
	defer l.Close()

	mockKV.EXPECT().Delete(ctx, "/ns/service/test/0000000000000017").Return(
		&clientv3.DeleteResponse{}, nil)

	err = exporter.UnexportPort(ctx)
	if err != nil {
		t.Error("UnexportPort reports error: ", err)
	}
}

func TestNewExportedPort_LeaseGrantFails(t *testing.T) {
	controller := gomock.NewController(t)
	mockKV := mock_etcd.NewMockKV(controller)
	mockLease := mock_etcd.NewMockLease(controller)
	ctx := context.TODO()

	errExpected := errors.New("No lease for thee")
	mockLease.EXPECT().Grant(gomock.Eq(ctx), gomock.Eq(int64(30000))).Return(
		nil, errExpected)

	exporter, err := NewExporterFromClient(ctx, mockKV, mockLease, 30000)
	if err != errExpected {
		t.Error("NewExporterFromClient reports unexpected error: ", err)
	}
	if exporter == nil {
		t.Fatal("NewExporterFromClient returned nil exporter")
	}
}

func TestNewExportedPort_LeaseKeepAliveFails(t *testing.T) {
	controller := gomock.NewController(t)
	mockKV := mock_etcd.NewMockKV(controller)
	mockLease := mock_etcd.NewMockLease(controller)
	ctx := context.TODO()

	mockLease.EXPECT().Grant(gomock.Eq(ctx), gomock.Eq(int64(30000))).Return(
		&clientv3.LeaseGrantResponse{
			ID:    23,
			TTL:   30000,
			Error: "",
		}, nil)
	mockLease.EXPECT().KeepAlive(
		gomock.Eq(context.Background()),
		gomock.Eq(clientv3.LeaseID(23))).Return(
		nil, errors.New("It's dead, Jim"))

	exporter, err := NewExporterFromClient(ctx, mockKV, mockLease, 30000)
	if err != nil {
		t.Error("NewExporterFromClient reports unexpected error: ", err)
	}
	if exporter == nil {
		t.Fatal("NewExporterFromClient returned nil exporter")
	}
}

func TestNewExportedPort_PutFails(t *testing.T) {
	controller := gomock.NewController(t)
	mockKV := mock_etcd.NewMockKV(controller)
	mockLease := mock_etcd.NewMockLease(controller)
	ctx := context.TODO()

	mockLease.EXPECT().Grant(gomock.Eq(ctx), gomock.Eq(int64(30000))).Return(
		&clientv3.LeaseGrantResponse{
			ID:    23,
			TTL:   30000,
			Error: "",
		}, nil)
	mockLease.EXPECT().KeepAlive(
		gomock.Eq(context.Background()),
		gomock.Eq(clientv3.LeaseID(23))).Return(
		make(chan *clientv3.LeaseKeepAliveResponse), nil)

	exporter, err := NewExporterFromClient(ctx, mockKV, mockLease, 30000)
	if err != nil {
		t.Error("NewExporterFromClient reports error: ", err)
	}
	if exporter == nil {
		t.Fatal("NewExporterFromClient returned nil exporter")
	}

	record := discovery.ExportedServiceRecord{
		Protocol: "tcp",
		Address:  "127.0.0.1",
		Port:     23456,
	}
	bData, err := proto.Marshal(&record)
	if err != nil {
		t.Error("Unable to marshal test ExportedServiceRecord proto")
	}

	errExpected := errors.New("etcd didn't like your value")
	mockKV.EXPECT().Put(ctx, "/ns/service/test/0000000000000017", string(bData),
		gomock.Any()).Return(nil, errExpected)

	l, err := exporter.NewExportedPort(ctx, "tcp", "127.0.0.1:23456", "test")
	if err != errExpected {
		t.Error("NewExportedPort reports unexpected error: ", err)
	}
	if l != nil {
		t.Fatal("NewExportedPort returned non-nil listener")
	}

	/*
	   Call should not actually invoke delete as we did not successfully
	   export anything.
	*/
	err = exporter.UnexportPort(ctx)
	if err != nil {
		t.Error("UnexportPort reports error: ", err)
	}
}

func TestNewExportedTLSPort_PutFails(t *testing.T) {
	controller := gomock.NewController(t)
	mockKV := mock_etcd.NewMockKV(controller)
	mockLease := mock_etcd.NewMockLease(controller)
	ctx := context.TODO()

	mockLease.EXPECT().Grant(gomock.Eq(ctx), gomock.Eq(int64(30000))).Return(
		&clientv3.LeaseGrantResponse{
			ID:    23,
			TTL:   30000,
			Error: "",
		}, nil)
	mockLease.EXPECT().KeepAlive(
		gomock.Eq(context.Background()),
		gomock.Eq(clientv3.LeaseID(23))).Return(
		make(chan *clientv3.LeaseKeepAliveResponse), nil)

	exporter, err := NewExporterFromClient(ctx, mockKV, mockLease, 30000)
	if err != nil {
		t.Error("NewExporterFromClient reports error: ", err)
	}
	if exporter == nil {
		t.Fatal("NewExporterFromClient returned nil exporter")
	}

	record := discovery.ExportedServiceRecord{
		Protocol: "tcp",
		Address:  "127.0.0.1",
		Port:     23456,
	}
	bData, err := proto.Marshal(&record)
	if err != nil {
		t.Error("Unable to marshal test ExportedServiceRecord proto")
	}

	errExpected := errors.New("etcd didn't like your value")
	mockKV.EXPECT().Put(ctx, "/ns/service/test/0000000000000017", string(bData),
		gomock.Any()).Return(nil, errExpected)

	l, err := exporter.NewExportedTLSPort(
		ctx, "tcp", "127.0.0.1:23456", "test", &tls.Config{})
	if err != errExpected {
		t.Error("NewExportedPort reports unexpected error: ", err)
	}
	if l != nil {
		t.Fatal("NewExportedPort returned non-nil listener")
	}
}

func TestNewExportedPort_DeleteFails(t *testing.T) {
	controller := gomock.NewController(t)
	mockKV := mock_etcd.NewMockKV(controller)
	mockLease := mock_etcd.NewMockLease(controller)
	ctx := context.TODO()

	mockLease.EXPECT().Grant(gomock.Eq(ctx), gomock.Eq(int64(30000))).Return(
		&clientv3.LeaseGrantResponse{
			ID:    23,
			TTL:   30000,
			Error: "",
		}, nil)
	mockLease.EXPECT().KeepAlive(
		gomock.Eq(context.Background()),
		gomock.Eq(clientv3.LeaseID(23))).Return(
		make(chan *clientv3.LeaseKeepAliveResponse), nil)

	exporter, err := NewExporterFromClient(ctx, mockKV, mockLease, 30000)
	if err != nil {
		t.Error("NewExporterFromClient reports error: ", err)
	}
	if exporter == nil {
		t.Fatal("NewExporterFromClient returned nil exporter")
	}

	record := discovery.ExportedServiceRecord{
		Protocol: "tcp",
		Address:  "127.0.0.1",
		Port:     23456,
	}
	bData, err := proto.Marshal(&record)
	if err != nil {
		t.Error("Unable to marshal test ExportedServiceRecord proto")
	}

	mockKV.EXPECT().Put(ctx, "/ns/service/test/0000000000000017", string(bData),
		gomock.Any()).Return(&clientv3.PutResponse{}, nil)

	l, err := exporter.NewExportedPort(ctx, "tcp", "127.0.0.1:23456", "test")
	if err != nil {
		t.Error("NewExportedPort reports error: ", err)
	}
	if l == nil {
		t.Fatal("NewExportedPort returned nil listener")
	}
	defer l.Close()

	errExpected := errors.New("etcd does not like your request")
	mockKV.EXPECT().Delete(ctx, "/ns/service/test/0000000000000017").Return(
		nil, errExpected)

	err = exporter.UnexportPort(ctx)
	if err != errExpected {
		t.Error("UnexportPort reports unexpected error: ", err)
	}
}

func TestNewExportedPort_RandomHostPort(t *testing.T) {
	controller := gomock.NewController(t)
	mockKV := mock_etcd.NewMockKV(controller)
	mockLease := mock_etcd.NewMockLease(controller)
	ctx := context.TODO()

	mockLease.EXPECT().Grant(gomock.Eq(ctx), gomock.Eq(int64(30000))).Return(
		&clientv3.LeaseGrantResponse{
			ID:    23,
			TTL:   30000,
			Error: "",
		}, nil)
	mockLease.EXPECT().KeepAlive(
		gomock.Eq(context.Background()),
		gomock.Eq(clientv3.LeaseID(23))).Return(
		make(chan *clientv3.LeaseKeepAliveResponse), nil)

	exporter, err := NewExporterFromClient(ctx, mockKV, mockLease, 30000)
	if err != nil {
		t.Error("NewExporterFromClient reports error: ", err)
	}
	if exporter == nil {
		t.Fatal("NewExporterFromClient returned nil exporter")
	}

	record := discovery.ExportedServiceRecord{
		Protocol: "tcp",
		Address:  "127.0.0.1",
	}
	recvRecord := discovery.ExportedServiceRecord{}

	mockKV.EXPECT().Put(ctx, "/ns/service/test/0000000000000017", gomock.Any(),
		gomock.Any()).Return(&clientv3.PutResponse{}, nil).Do(
		func(ctx context.Context, path string, val string,
			opts ...clientv3.OpOption) {
			otherErr := proto.Unmarshal([]byte(val), &recvRecord)
			if otherErr != nil {
				t.Error("Error parsing protobuf message from Put: ", otherErr)
			}
		})

	l, err := exporter.NewExportedPort(ctx, "tcp", "127.0.0.1", "test")
	if err != nil {
		t.Error("NewExportedPort reports error: ", err)
	}
	if l == nil {
		t.Fatal("NewExportedPort returned nil listener")
	}
	defer l.Close()

	_, hostport, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Error("Error parsing host:port pair ", l.Addr().String(), ": ", err)
	}
	port, err := strconv.Atoi(hostport)
	if err != nil {
		t.Error("Error converting port ", hostport, ": ", err)
	}

	record.Port = int32(port)
	if !proto.Equal(&record, &recvRecord) {
		t.Error("Mismatch between proto records: ", record, ", ", recvRecord)
	}
}
