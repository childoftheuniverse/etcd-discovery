package discovery

import (
	"fmt"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

/*
ExportedServiceNotificationReceiver is an interface used by the exported service
monitor to notify the caller of changes.
*/
type ExportedServiceNotificationReceiver interface {
	/*
	   ReportChange is used to indicate to the caller that a change has occurred to
	   the list of backends. The details of the change are transported in the
	   ExportedServiceUpdateNotification.
	*/
	ReportChange(*ExportedServiceUpdateNotification)

	/*
	   ReportError is used to indicate a non-fatal error to the caller.
	*/
	ReportError(error)

	/*
	   ReportFatal is used to indicate a fatal error to the caller. After this
	   event is fired, listening to modifications will stop.
	*/
	ReportFatal(error)

	/*
	   ReportCancelled reports that the watcher has been cancelled, which can be
	   used as a signal to the notification receiver to shut down.
	*/
	ReportCancelled()
}

/*
exportedServiceMonitor is used internally to keep track of watchers tracking
etcd paths for service changes.
*/
type exportedServiceMonitor struct {
	basePath    string
	etcdKV      etcd.KV
	etcdWatcher etcd.Watcher
	receiver    ExportedServiceNotificationReceiver
}

/*
MonitorExportedService creates a new monitor for updates on the given
baseString in the specified etcdClient. See the documentation of the
ExportedServiceNotificationReceiver interface for more details.
*/
func MonitorExportedService(
	etcdClient *etcd.Client, basePath string,
	receiver ExportedServiceNotificationReceiver) {
	var mon = &exportedServiceMonitor{
		basePath:    basePath,
		etcdKV:      etcdClient,
		etcdWatcher: etcdClient,
		receiver:    receiver,
	}

	go mon.monitor()
}

/*
StopMonitoringExportedServices stops monitoring _all_ exported services as well
as any other watchers registered on the specified etcd server.
*/
func StopMonitoringExportedServices(watcher etcd.Watcher) error {
	return watcher.Close()
}

/*
monitor tracks changes in the specified subdirectory and sends out notifications
to the caller. This uses callbacks over channels to save on threads.
*/
func (r *exportedServiceMonitor) monitor() {
	var resp *etcd.GetResponse
	var ch etcd.WatchChan
	var wr etcd.WatchResponse
	var val *mvccpb.KeyValue
	var err error

	/*
	   First, determine the current state of all objects in the tree.
	*/
	if resp, err = r.etcdKV.Get(context.Background(), r.basePath,
		etcd.WithPrefix()); err != nil {
		r.receiver.ReportError(err)
	}

	for _, val = range resp.Kvs {
		var service = new(ExportedServiceRecord)
		var notification ExportedServiceUpdateNotification

		err = proto.Unmarshal(val.Value, service)
		if err != nil {
			r.receiver.ReportError(err)
			continue
		}

		notification = ExportedServiceUpdateNotification{
			Path:        string(val.Key),
			Update:      ExportedServiceUpdateNotification_NEW,
			UpdatedData: service,
		}
		r.receiver.ReportChange(&notification)
	}

	/*
	   Start watching for changes on the specified prefix.
	*/
	ch = r.etcdWatcher.Watch(context.Background(), r.basePath,
		etcd.WithPrefix(), etcd.WithPrevKV())

	for wr = range ch {
		var ev *etcd.Event

		if wr.Err() != nil {
			if wr.Canceled {
				r.receiver.ReportFatal(wr.Err())
			} else {
				r.receiver.ReportError(wr.Err())
			}
			continue
		}

		if wr.Canceled {
			// We can assume the event is a pure cancellation as otherwise
			// the above wr.Err() code would have triggered.
			r.receiver.ReportCancelled()
			return
		}

		for _, ev = range wr.Events {
			val = ev.Kv

			if ev.IsCreate() {
				var service = new(ExportedServiceRecord)
				var notification ExportedServiceUpdateNotification

				err = proto.Unmarshal(val.Value, service)
				if err != nil {
					r.receiver.ReportError(err)
					continue
				}

				notification = ExportedServiceUpdateNotification{
					Path:        string(val.Key),
					Update:      ExportedServiceUpdateNotification_NEW,
					UpdatedData: service,
				}
				r.receiver.ReportChange(&notification)
			} else if ev.Type == mvccpb.DELETE {
				// Why is there no IsDelete()?!
				var service = new(ExportedServiceRecord)
				err = proto.Unmarshal(ev.PrevKv.Value, service)
				if err != nil {
					r.receiver.ReportError(err)
				}

				var notification = ExportedServiceUpdateNotification{
					Path:        string(val.Key),
					Update:      ExportedServiceUpdateNotification_DELETED,
					UpdatedData: service,
				}
				r.receiver.ReportChange(&notification)
			} else {
				r.receiver.ReportError(fmt.Errorf(
					"Caught event I have no idea about: %v", ev))
			}
		}
	}
}
