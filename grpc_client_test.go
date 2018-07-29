package discovery

import (
	"context"
	"errors"
	"testing"

	"github.com/childoftheuniverse/etcd-discovery/testing/mock_etcd"
	"github.com/childoftheuniverse/etcd-discovery/testing/mock_grpc"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

func TestNewGrpcClient_PartialPath_Success(t *testing.T) {
	controller := gomock.NewController(t)
	oldGrpcDialContext := grpc.DialContext
	mockDialer := mock_grpc.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	grpcDialContext = mockDialer.DialContext
	defer func() { grpcDialContext = oldGrpcDialContext }()

	ctx := context.TODO()

	service := ExportedServiceRecord{
		Protocol: "test",
		Address:  "localhost",
		Port:     423,
	}
	pbData, err := proto.Marshal(&service)
	if err != nil {
		t.Error("Unable to encode test data: ", err)
	}

	mockKV.EXPECT().Get(ctx, "/ns/service/test", gomock.Any()).Return(
		&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				&mvccpb.KeyValue{
					Key:   []byte("/ns/service/test"),
					Value: pbData,
				},
			},
			Count: 1,
		}, nil)
	mockDialer.EXPECT().DialContext(ctx, "localhost:423").Return(
		&grpc.ClientConn{}, nil)

	conn, err := NewGrpcClient(ctx, mockKV, "test")
	if err != nil {
		t.Error("Received unexpected error from NewGrpcClient: ", err)
	}
	if conn == nil {
		t.Error("Received nil connection")
	}
}

func TestNewGrpcClient_FullPath_Success(t *testing.T) {
	controller := gomock.NewController(t)
	oldGrpcDialContext := grpc.DialContext
	mockDialer := mock_grpc.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	grpcDialContext = mockDialer.DialContext
	defer func() { grpcDialContext = oldGrpcDialContext }()

	ctx := context.TODO()

	service := ExportedServiceRecord{
		Protocol: "test",
		Address:  "localhost",
		Port:     423,
	}
	pbData, err := proto.Marshal(&service)
	if err != nil {
		t.Error("Unable to encode test data: ", err)
	}

	mockKV.EXPECT().Get(ctx, "/this/is/a/full/path", gomock.Any()).Return(
		&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				&mvccpb.KeyValue{
					Key:   []byte("/this/is/a/full/path"),
					Value: pbData,
				},
			},
			Count: 1,
		}, nil)
	mockDialer.EXPECT().DialContext(ctx, "localhost:423").Return(
		&grpc.ClientConn{}, nil)

	conn, err := NewGrpcClient(ctx, mockKV, "/this/is/a/full/path")
	if err != nil {
		t.Error("Received unexpected error from NewGrpcClient: ", err)
	}
	if conn == nil {
		t.Error("Received nil connection")
	}
}

func TestNewGrpcClient_FullPathWithTrailingSlash(t *testing.T) {
	controller := gomock.NewController(t)
	oldGrpcDialContext := grpc.DialContext
	mockDialer := mock_grpc.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	grpcDialContext = mockDialer.DialContext
	defer func() { grpcDialContext = oldGrpcDialContext }()

	ctx := context.TODO()

	service := ExportedServiceRecord{
		Protocol: "test",
		Address:  "localhost",
		Port:     423,
	}
	pbData, err := proto.Marshal(&service)
	if err != nil {
		t.Error("Unable to encode test data: ", err)
	}

	mockKV.EXPECT().Get(ctx, "/this/is/a/full/", gomock.Any()).Return(
		&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				&mvccpb.KeyValue{
					Key:   []byte("/this/is/a/full/path"),
					Value: pbData,
				},
			},
			Count: 1,
		}, nil)
	mockDialer.EXPECT().DialContext(ctx, "localhost:423").Return(
		&grpc.ClientConn{}, nil)

	conn, err := NewGrpcClient(ctx, mockKV, "/this/is/a/full/")
	if err != nil {
		t.Error("Received unexpected error from NewGrpcClient: ", err)
	}
	if conn == nil {
		t.Error("Received nil connection")
	}
}

func TestNewGrpcClient_MultiplePaths(t *testing.T) {
	controller := gomock.NewController(t)
	oldGrpcDialContext := grpc.DialContext
	mockDialer := mock_grpc.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	grpcDialContext = mockDialer.DialContext
	defer func() { grpcDialContext = oldGrpcDialContext }()

	ctx := context.TODO()

	service1 := ExportedServiceRecord{
		Protocol: "test",
		Address:  "localhost",
		Port:     423,
	}
	pbData1, err := proto.Marshal(&service1)
	if err != nil {
		t.Error("Unable to encode test data: ", err)
	}
	service2 := ExportedServiceRecord{
		Protocol: "test",
		Address:  "localhost",
		Port:     345,
	}
	pbData2, err := proto.Marshal(&service2)
	if err != nil {
		t.Error("Unable to encode test data: ", err)
	}

	mockKV.EXPECT().Get(ctx, "/ns/service/test", gomock.Any()).Return(
		&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				&mvccpb.KeyValue{
					Key:   []byte("/ns/service/test/a"),
					Value: pbData1,
				},
				&mvccpb.KeyValue{
					Key:   []byte("/ns/service/test/b"),
					Value: pbData2,
				},
			},
			Count: 2,
		}, nil)
	mockDialer.EXPECT().DialContext(ctx, "localhost:423").Return(
		nil, errors.New("That didn't work bruh"))
	mockDialer.EXPECT().DialContext(ctx, "localhost:345").Return(
		&grpc.ClientConn{}, nil)

	conn, err := NewGrpcClient(ctx, mockKV, "/ns/service/test")
	if err != nil {
		t.Error("Received unexpected error from NewGrpcClient: ", err)
	}
	if conn == nil {
		t.Error("Received nil connection")
	}
}

func TestNewGrpcClient_EtcdFails(t *testing.T) {
	controller := gomock.NewController(t)
	oldGrpcDialContext := grpc.DialContext
	mockDialer := mock_grpc.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	grpcDialContext = mockDialer.DialContext
	defer func() { grpcDialContext = oldGrpcDialContext }()

	ctx := context.TODO()

	mockKV.EXPECT().Get(ctx, "/ns/service/test", gomock.Any()).Return(
		nil, errors.New("etcd is sad"))

	conn, err := NewGrpcClient(ctx, mockKV, "test")
	if err == nil {
		t.Error("NewGrpcClient succeeds despite etcd errors?")
	}
	if conn != nil {
		t.Error("Received non-nil connection")
	}
}

func TestNewGrpcClient_EtcdContentsGarbled(t *testing.T) {
	controller := gomock.NewController(t)
	oldGrpcDialContext := grpc.DialContext
	mockDialer := mock_grpc.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	grpcDialContext = mockDialer.DialContext
	defer func() { grpcDialContext = oldGrpcDialContext }()

	ctx := context.TODO()

	mockKV.EXPECT().Get(ctx, "/ns/service/test", gomock.Any()).Return(
		&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				&mvccpb.KeyValue{
					Key:   []byte("/ns/service/test"),
					Value: []byte("Whazzup bruh"),
				},
			},
			Count: 1,
		}, nil)

	conn, err := NewGrpcClient(ctx, mockKV, "test")
	if err == nil {
		t.Error("NewGrpcClient succeeds despite expected decoding errors?")
	}
	if conn != nil {
		t.Error("Received non-nil connection")
	}
}

func TestNewGrpcClient_EtcdNoResults(t *testing.T) {
	controller := gomock.NewController(t)
	oldGrpcDialContext := grpc.DialContext
	mockDialer := mock_grpc.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	grpcDialContext = mockDialer.DialContext
	defer func() { grpcDialContext = oldGrpcDialContext }()

	ctx := context.TODO()

	mockKV.EXPECT().Get(ctx, "/ns/service/test", gomock.Any()).Return(
		&clientv3.GetResponse{
			Kvs:   []*mvccpb.KeyValue{},
			Count: 1,
		}, nil)

	conn, err := NewGrpcClient(ctx, mockKV, "test")
	if err == nil {
		t.Error("NewGrpcClient succeeds despite expected decoding errors?")
	}
	if conn != nil {
		t.Error("Received non-nil connection")
	}
}

func TestNewGrpcClient_ConnectionFailed(t *testing.T) {
	controller := gomock.NewController(t)
	oldGrpcDialContext := grpc.DialContext
	mockDialer := mock_grpc.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	grpcDialContext = mockDialer.DialContext
	defer func() { grpcDialContext = oldGrpcDialContext }()

	ctx := context.TODO()

	service := ExportedServiceRecord{
		Protocol: "test",
		Address:  "localhost",
		Port:     423,
	}
	pbData, err := proto.Marshal(&service)
	if err != nil {
		t.Error("Unable to encode test data: ", err)
	}

	mockKV.EXPECT().Get(ctx, "/ns/service/test", gomock.Any()).Return(
		&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				&mvccpb.KeyValue{
					Key:   []byte("/ns/service/test"),
					Value: pbData,
				},
			},
			Count: 1,
		}, nil)
	mockDialer.EXPECT().DialContext(ctx, "localhost:423").Return(
		nil, errors.New("Sad, sad network"))

	conn, err := NewGrpcClient(ctx, mockKV, "test")
	if err == nil {
		t.Error("NewGrpcClient succeeds despite expected connection errors?")
	}
	if conn != nil {
		t.Error("Received non-nil connection")
	}
}
