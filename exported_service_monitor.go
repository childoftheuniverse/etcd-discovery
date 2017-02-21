package discovery

import (
	"context"
	"fmt"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
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
}

/*
exportedServiceMonitor is used internally to keep track of watchers tracking
etcd paths for service changes.
*/
type exportedServiceMonitor struct {
	basePath   string
	etcdClient *etcd.Client
	receiver   ExportedServiceNotificationReceiver
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
		basePath:   basePath,
		etcdClient: etcdClient,
		receiver:   receiver,
	}

	go mon.monitor()
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
	if resp, err = r.etcdClient.Get(context.Background(), r.basePath,
		etcd.WithPrefix()); err != nil {
		r.receiver.ReportError(err)
	}

	for _, val = range resp.Kvs {
		var notification = ExportedServiceUpdateNotification{
			Path:        string(val.Key),
			Update:      ExportedServiceUpdateNotification_NEW,
			UpdatedData: val.Value,
		}
		r.receiver.ReportChange(&notification)
	}

	/*
	   Start watching for changes on the specified prefix.
	*/
	ch = r.etcdClient.Watch(context.Background(), r.basePath, etcd.WithPrefix(),
		etcd.WithPrevKV())

	for wr = range ch {
		var ev *etcd.Event

		if wr.Err() != nil {
			r.receiver.ReportError(wr.Err())
			continue
		}

		if wr.Canceled {
			r.receiver.ReportFatal(fmt.Errorf(
				"etcd exported node discovery watcher canceled"))
			return
		}

		for _, ev = range wr.Events {
			if ev.IsCreate() {
				var notification = ExportedServiceUpdateNotification{
					Path:        string(ev.Kv.Key),
					Update:      ExportedServiceUpdateNotification_NEW,
					UpdatedData: ev.Kv.Value,
				}
				r.receiver.ReportChange(&notification)
			} else if ev.Type == mvccpb.DELETE {
				// Why is there no IsDelete()?!
				var notification = ExportedServiceUpdateNotification{
					Path:        string(ev.Kv.Key),
					Update:      ExportedServiceUpdateNotification_DELETED,
					UpdatedData: ev.Kv.Value,
				}
				r.receiver.ReportChange(&notification)
			} else {
				r.receiver.ReportError(fmt.Errorf(
					"Caught event I have no idea about: %v", ev))
			}
		}
	}
}
