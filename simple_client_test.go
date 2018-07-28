package discovery

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/childoftheuniverse/etcd-discovery/testing/mock_etcd"
	"github.com/childoftheuniverse/etcd-discovery/testing/mock_net"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
)

func TestNewSimpleClient_PartialPath_Success(t *testing.T) {
	controller := gomock.NewController(t)
	oldNetDial := net.Dial
	mockDialer := mock_net.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	mockConnection := mock_net.NewMockConn(controller)
	netDial = mockDialer.Dial
	defer func() { netDial = oldNetDial }()

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
	mockDialer.EXPECT().Dial("test", "localhost:423").Return(
		mockConnection, nil)

	conn, err := NewSimpleClient(ctx, mockKV, "test")
	if err != nil {
		t.Error("Received unexpected error from NewSimpleClient: ", err)
	}
	if conn == nil {
		t.Error("Received nil connection")
	}
}

func TestNewSimpleClient_FullPath_Success(t *testing.T) {
	controller := gomock.NewController(t)
	oldNetDial := net.Dial
	mockDialer := mock_net.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	mockConnection := mock_net.NewMockConn(controller)
	netDial = mockDialer.Dial
	defer func() { netDial = oldNetDial }()

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
	mockDialer.EXPECT().Dial("test", "localhost:423").Return(
		mockConnection, nil)

	conn, err := NewSimpleClient(ctx, mockKV, "/this/is/a/full/path")
	if err != nil {
		t.Error("Received unexpected error from NewSimpleClient: ", err)
	}
	if conn == nil {
		t.Error("Received nil connection")
	}
}

func TestNewSimpleClient_MultiplePaths(t *testing.T) {
	controller := gomock.NewController(t)
	oldNetDial := net.Dial
	mockDialer := mock_net.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	mockConnection := mock_net.NewMockConn(controller)
	netDial = mockDialer.Dial
	defer func() { netDial = oldNetDial }()

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
	mockDialer.EXPECT().Dial("test", "localhost:423").Return(
		nil, errors.New("That didn't work bruh"))
	mockDialer.EXPECT().Dial("test", "localhost:345").Return(
		mockConnection, nil)

	conn, err := NewSimpleClient(ctx, mockKV, "/ns/service/test")
	if err != nil {
		t.Error("Received unexpected error from NewSimpleClient: ", err)
	}
	if conn == nil {
		t.Error("Received nil connection")
	}
}

func TestNewSimpleClient_EtcdFails(t *testing.T) {
	controller := gomock.NewController(t)
	oldNetDial := net.Dial
	mockDialer := mock_net.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	netDial = mockDialer.Dial
	defer func() { netDial = oldNetDial }()

	ctx := context.TODO()

	mockKV.EXPECT().Get(ctx, "/ns/service/test", gomock.Any()).Return(
		nil, errors.New("etcd is sad"))

	conn, err := NewSimpleClient(ctx, mockKV, "test")
	if err == nil {
		t.Error("NewSimpleClient succeeds despite etcd errors?")
	}
	if conn != nil {
		t.Error("Received non-nil connection")
	}
}

func TestNewSimpleClient_EtcdContentsGarbled(t *testing.T) {
	controller := gomock.NewController(t)
	oldNetDial := net.Dial
	mockDialer := mock_net.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	netDial = mockDialer.Dial
	defer func() { netDial = oldNetDial }()

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

	conn, err := NewSimpleClient(ctx, mockKV, "test")
	if err == nil {
		t.Error("NewSimpleClient succeeds despite expected decoding errors?")
	}
	if conn != nil {
		t.Error("Received non-nil connection")
	}
}

func TestNewSimpleClient_ConnectionFailed(t *testing.T) {
	controller := gomock.NewController(t)
	oldNetDial := net.Dial
	mockDialer := mock_net.NewMockDialer(controller)
	mockKV := mock_etcd.NewMockKV(controller)
	netDial = mockDialer.Dial
	defer func() { netDial = oldNetDial }()

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
	mockDialer.EXPECT().Dial("test", "localhost:423").Return(
		nil, errors.New("Sad, sad network"))

	conn, err := NewSimpleClient(ctx, mockKV, "test")
	if err == nil {
		t.Error("NewSimpleClient succeeds despite expected connection errors?")
	}
	if conn != nil {
		t.Error("Received non-nil connection")
	}
}
