package discovery

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/childoftheuniverse/etcd-discovery/testing/mock_etcd"
	"github.com/childoftheuniverse/proto-testing"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
)

// MockExportedServiceNotificationReceiver is a mock of ExportedServiceNotificationReceiver interface
type MockExportedServiceNotificationReceiver struct {
	ctrl     *gomock.Controller
	recorder *MockExportedServiceNotificationReceiverMockRecorder
}

// MockExportedServiceNotificationReceiverMockRecorder is the mock recorder for MockExportedServiceNotificationReceiver
type MockExportedServiceNotificationReceiverMockRecorder struct {
	mock *MockExportedServiceNotificationReceiver
}

// NewMockExportedServiceNotificationReceiver creates a new mock instance
func NewMockExportedServiceNotificationReceiver(ctrl *gomock.Controller) *MockExportedServiceNotificationReceiver {
	mock := &MockExportedServiceNotificationReceiver{ctrl: ctrl}
	mock.recorder = &MockExportedServiceNotificationReceiverMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockExportedServiceNotificationReceiver) EXPECT() *MockExportedServiceNotificationReceiverMockRecorder {
	return m.recorder
}

// ReportCancelled mocks base method
func (m *MockExportedServiceNotificationReceiver) ReportCancelled() {
	m.ctrl.Call(m, "ReportCancelled")
}

// ReportCancelled indicates an expected call of ReportCancelled
func (mr *MockExportedServiceNotificationReceiverMockRecorder) ReportCancelled() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReportCancelled", reflect.TypeOf((*MockExportedServiceNotificationReceiver)(nil).ReportCancelled))
}

// ReportChange mocks base method
func (m *MockExportedServiceNotificationReceiver) ReportChange(arg0 *ExportedServiceUpdateNotification) {
	m.ctrl.Call(m, "ReportChange", arg0)
}

// ReportChange indicates an expected call of ReportChange
func (mr *MockExportedServiceNotificationReceiverMockRecorder) ReportChange(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReportChange", reflect.TypeOf((*MockExportedServiceNotificationReceiver)(nil).ReportChange), arg0)
}

// ReportError mocks base method
func (m *MockExportedServiceNotificationReceiver) ReportError(arg0 error) {
	m.ctrl.Call(m, "ReportError", arg0)
}

// ReportError indicates an expected call of ReportError
func (mr *MockExportedServiceNotificationReceiverMockRecorder) ReportError(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReportError", reflect.TypeOf((*MockExportedServiceNotificationReceiver)(nil).ReportError), arg0)
}

// ReportFatal mocks base method
func (m *MockExportedServiceNotificationReceiver) ReportFatal(arg0 error) {
	m.ctrl.Call(m, "ReportFatal", arg0)
}

// ReportFatal indicates an expected call of ReportFatal
func (mr *MockExportedServiceNotificationReceiverMockRecorder) ReportFatal(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReportFatal", reflect.TypeOf((*MockExportedServiceNotificationReceiver)(nil).ReportFatal), arg0)
}

func TestMonitorExportedService_AddRecord(t *testing.T) {
	donech := make(chan bool)

	wc := make(chan clientv3.WatchResponse)
	controller := gomock.NewController(t)

	mockKV := mock_etcd.NewMockKV(controller)
	mockWatcher := mock_etcd.NewMockWatcher(controller)
	receiver := NewMockExportedServiceNotificationReceiver(
		controller)
	ctx := context.Background()

	service := ExportedServiceRecord{
		Protocol: "test",
		Address:  "localhost",
		Port:     423,
	}
	pbData, err := proto.Marshal(&service)
	if err != nil {
		t.Error("Unable to encode test data: ", err)
	}

	mockKV.EXPECT().Get(gomock.Eq(ctx),
		"/ns/service/test", gomock.Any()).Return(
		&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				&mvccpb.KeyValue{
					Key:            []byte("/ns/service/test/foo"),
					Value:          pbData,
					CreateRevision: 2,
					ModRevision:    2,
				},
			},
			Count: 1,
		}, nil)
	receiver.EXPECT().ReportChange(
		proto_testing.ProtoEq(&ExportedServiceUpdateNotification{
			Path:        "/ns/service/test/foo",
			Update:      ExportedServiceUpdateNotification_NEW,
			UpdatedData: &service,
		}))
	mockWatcher.EXPECT().Watch(
		gomock.Eq(context.Background()),
		"/ns/service/test", gomock.Any()).Return(wc)
	receiver.EXPECT().ReportChange(
		proto_testing.ProtoEq(&ExportedServiceUpdateNotification{
			Path:        "/ns/service/test/bar",
			Update:      ExportedServiceUpdateNotification_NEW,
			UpdatedData: &service,
		}))
	receiver.EXPECT().ReportFatal(gomock.Any()).Do(func(err error) {
		donech <- true
	})
	mockWatcher.EXPECT().Close()

	go func() {
		wc <- clientv3.WatchResponse{
			Events:          []*clientv3.Event{},
			Canceled:        false,
			Created:         true,
			CompactRevision: 0,
		}
		wc <- clientv3.WatchResponse{
			Events: []*clientv3.Event{
				&clientv3.Event{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Key:            []byte("/ns/service/test/bar"),
						Value:          pbData,
						CreateRevision: 3,
						ModRevision:    3,
					},
				},
			},
			Canceled:        false,
			Created:         false,
			CompactRevision: 0,
		}
		wc <- clientv3.WatchResponse{
			Events:          []*clientv3.Event{},
			Canceled:        true,
			Created:         false,
			CompactRevision: 0,
		}
	}()

	MonitorExportedService(mockKV, mockWatcher, "test", receiver)

	select {
	case <-donech:
		break
	case <-time.After(10 * time.Second):
		t.Fatalf("Expected call to ReportCancelled, which never arrived")
	}
	StopMonitoringExportedServices(mockWatcher)
}

func TestMonitorExportedService_RemoveRecord(t *testing.T) {
	donech := make(chan bool)

	wc := make(chan clientv3.WatchResponse)
	controller := gomock.NewController(t)

	mockKV := mock_etcd.NewMockKV(controller)
	mockWatcher := mock_etcd.NewMockWatcher(controller)
	receiver := NewMockExportedServiceNotificationReceiver(
		controller)
	ctx := context.Background()

	service := ExportedServiceRecord{
		Protocol: "test",
		Address:  "localhost",
		Port:     423,
	}
	pbData, err := proto.Marshal(&service)
	if err != nil {
		t.Error("Unable to encode test data: ", err)
	}

	mockKV.EXPECT().Get(gomock.Eq(ctx),
		"/ns/service/test", gomock.Any()).Return(
		&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				&mvccpb.KeyValue{
					Key:            []byte("/ns/service/test/foo"),
					Value:          pbData,
					CreateRevision: 2,
					ModRevision:    2,
				},
				&mvccpb.KeyValue{
					Key:            []byte("/ns/service/test/bar"),
					Value:          pbData,
					CreateRevision: 3,
					ModRevision:    3,
				},
			},
			Count: 1,
		}, nil)
	receiver.EXPECT().ReportChange(
		proto_testing.ProtoEq(&ExportedServiceUpdateNotification{
			Path:        "/ns/service/test/foo",
			Update:      ExportedServiceUpdateNotification_NEW,
			UpdatedData: &service,
		}))
	receiver.EXPECT().ReportChange(
		proto_testing.ProtoEq(&ExportedServiceUpdateNotification{
			Path:        "/ns/service/test/bar",
			Update:      ExportedServiceUpdateNotification_NEW,
			UpdatedData: &service,
		}))
	mockWatcher.EXPECT().Watch(
		gomock.Eq(context.Background()),
		"/ns/service/test", gomock.Any()).Return(wc)
	receiver.EXPECT().ReportChange(
		proto_testing.ProtoEq(&ExportedServiceUpdateNotification{
			Path:        "/ns/service/test/foo",
			Update:      ExportedServiceUpdateNotification_DELETED,
			UpdatedData: &service,
		}))
	receiver.EXPECT().ReportFatal(gomock.Any()).Do(func(err error) {
		donech <- true
	})
	mockWatcher.EXPECT().Close()

	go func() {
		wc <- clientv3.WatchResponse{
			Events:          []*clientv3.Event{},
			Canceled:        false,
			Created:         true,
			CompactRevision: 0,
		}
		wc <- clientv3.WatchResponse{
			Events: []*clientv3.Event{
				&clientv3.Event{
					Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{
						Key:            []byte("/ns/service/test/foo"),
						Value:          pbData,
						CreateRevision: 3,
						ModRevision:    3,
					},
				},
			},
			Canceled:        false,
			Created:         false,
			CompactRevision: 0,
		}
		wc <- clientv3.WatchResponse{
			Events:          []*clientv3.Event{},
			Canceled:        true,
			Created:         false,
			CompactRevision: 0,
		}
	}()

	MonitorExportedService(mockKV, mockWatcher, "test", receiver)

	select {
	case <-donech:
		break
	case <-time.After(10 * time.Second):
		t.Fatalf("Expected call to ReportCancelled, which never arrived")
	}
	StopMonitoringExportedServices(mockWatcher)
}

func TestMonitorExportedService_AbsolutePath(t *testing.T) {
	donech := make(chan bool)

	wc := make(chan clientv3.WatchResponse)
	controller := gomock.NewController(t)

	mockKV := mock_etcd.NewMockKV(controller)
	mockWatcher := mock_etcd.NewMockWatcher(controller)
	receiver := NewMockExportedServiceNotificationReceiver(
		controller)
	ctx := context.Background()

	mockKV.EXPECT().Get(gomock.Eq(ctx),
		"/full/path", gomock.Any()).Return(
		&clientv3.GetResponse{
			Kvs:   []*mvccpb.KeyValue{},
			Count: 1,
		}, nil)
	mockWatcher.EXPECT().Watch(
		gomock.Eq(context.Background()),
		"/full/path", gomock.Any()).Return(wc)
	receiver.EXPECT().ReportFatal(gomock.Any()).Do(func(err error) {
		donech <- true
	})
	mockWatcher.EXPECT().Close()

	go func() {
		wc <- clientv3.WatchResponse{
			Events:          []*clientv3.Event{},
			Canceled:        true,
			Created:         false,
			CompactRevision: 0,
		}
	}()

	MonitorExportedService(mockKV, mockWatcher, "/full/path", receiver)

	select {
	case <-donech:
		break
	case <-time.After(10 * time.Second):
		t.Fatalf("Expected call to ReportCancelled, which never arrived")
	}
	StopMonitoringExportedServices(mockWatcher)
}

func TestMonitorExportedService_GarbageProtos(t *testing.T) {
	donech := make(chan bool)

	wc := make(chan clientv3.WatchResponse)
	controller := gomock.NewController(t)

	mockKV := mock_etcd.NewMockKV(controller)
	mockWatcher := mock_etcd.NewMockWatcher(controller)
	receiver := NewMockExportedServiceNotificationReceiver(
		controller)
	ctx := context.Background()

	mockKV.EXPECT().Get(gomock.Eq(ctx),
		"/ns/service/test", gomock.Any()).Return(
		&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				&mvccpb.KeyValue{
					Key:            []byte("/ns/service/test/foo"),
					Value:          []byte("Wazzup bruh"),
					CreateRevision: 2,
					ModRevision:    2,
				},
			},
			Count: 1,
		}, nil)
	receiver.EXPECT().ReportError(gomock.Any())
	mockWatcher.EXPECT().Watch(
		gomock.Eq(context.Background()),
		"/ns/service/test", gomock.Any()).Return(wc)
	receiver.EXPECT().ReportError(gomock.Any()).Times(3)
	receiver.EXPECT().ReportFatal(gomock.Any()).Do(func(err error) {
		donech <- true
	})
	mockWatcher.EXPECT().Close()

	go func() {
		wc <- clientv3.WatchResponse{
			Events:          []*clientv3.Event{},
			Canceled:        false,
			Created:         true,
			CompactRevision: 0,
		}
		wc <- clientv3.WatchResponse{
			Events: []*clientv3.Event{
				&clientv3.Event{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Key:            []byte("/ns/service/test/bar"),
						Value:          []byte("Ya lisnin bruh?"),
						CreateRevision: 3,
						ModRevision:    3,
					},
				},
				&clientv3.Event{
					Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{
						Key:            []byte("/ns/service/test/foo"),
						Value:          []byte("...bruh?"),
						CreateRevision: 4,
						ModRevision:    4,
					},
				},
				&clientv3.Event{
					Type: -1,
				},
			},
			Canceled:        false,
			Created:         false,
			CompactRevision: 0,
		}
		wc <- clientv3.WatchResponse{
			Events:          []*clientv3.Event{},
			Canceled:        true,
			Created:         false,
			CompactRevision: 0,
		}
	}()

	MonitorExportedService(mockKV, mockWatcher, "test", receiver)

	select {
	case <-donech:
		break
	case <-time.After(10 * time.Second):
		t.Fatalf("Expected call to ReportCancelled, which never arrived")
	}
	StopMonitoringExportedServices(mockWatcher)
}

func TestMonitorExportedService_EtcdFlakingOut(t *testing.T) {
	donech := make(chan bool)

	wc := make(chan clientv3.WatchResponse)
	controller := gomock.NewController(t)

	mockKV := mock_etcd.NewMockKV(controller)
	mockWatcher := mock_etcd.NewMockWatcher(controller)
	receiver := NewMockExportedServiceNotificationReceiver(
		controller)
	ctx := context.Background()

	mockKV.EXPECT().Get(gomock.Eq(ctx),
		"/ns/service/test", gomock.Any()).Return(
		nil, errors.New("etcd is tired"))
	receiver.EXPECT().ReportError(gomock.Any())
	mockWatcher.EXPECT().Watch(
		gomock.Eq(context.Background()),
		"/ns/service/test", gomock.Any()).Return(wc)
	receiver.EXPECT().ReportFatal(gomock.Any()).Do(func(err error) {
		donech <- true
	})
	mockWatcher.EXPECT().Close()

	go func() {
		wc <- clientv3.WatchResponse{
			Events:          []*clientv3.Event{},
			Canceled:        true,
			Created:         false,
			CompactRevision: 0,
		}
	}()

	MonitorExportedService(mockKV, mockWatcher, "test", receiver)

	select {
	case <-donech:
		break
	case <-time.After(10 * time.Second):
		t.Fatalf("Expected call to ReportCancelled, which never arrived")
	}
	StopMonitoringExportedServices(mockWatcher)
}
