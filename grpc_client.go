package discovery

import (
	"net"
	"strconv"
	"strings"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var grpcDialContext = grpc.DialContext

/*
NewGrpcClient attempts to establish grpc client connections with the specified
options to the endpoints connected with the etcd prefix. If multiple
destinations are linked to the destination (e.g. through a service
subdirectory), a connection is attempted to each backend in random order.
In case of connection errors the target is skipped and the next one is
attempted.

Once established, aborted connections will not be reestablished to a different
endpoint.
*/
func NewGrpcClient(ctx context.Context, client etcd.KV, path string,
	opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	var configs []*ExportedServiceRecord
	var configPerms []int
	var getResponse *etcd.GetResponse
	var realPath, slashPath string
	var conn *grpc.ClientConn
	var err error

	if path[0] == '/' {
		realPath = path
	} else {
		realPath = "/ns/service/" + realPath
	}

	getResponse, err = client.Get(ctx, realPath, etcd.WithPrefix())
	if err != nil {
		return nil, err
	}

	if realPath[len(realPath)-1] == '/' {
		slashPath = realPath
	} else {
		slashPath = realPath + "/"
	}

	for _, kv := range getResponse.Kvs {
		if string(kv.Key) == realPath || strings.HasPrefix(string(kv.Key), slashPath) {
			var config = new(ExportedServiceRecord)
			err = proto.Unmarshal(kv.Value, config)
			if err == nil {
				configs = append(configs, config)
			}
		}
	}

	if len(configs) == 0 {
		if err == nil {
			err = grpc.Errorf(codes.Unavailable, "No services were found at %s",
				realPath)
		}

		return nil, err
	}

	// Try to connect to different ports at random.
	configPerms = make([]int, len(configs))
	for _, i := range configPerms {
		var config = configs[i]
		var dest = net.JoinHostPort(
			config.Address, strconv.Itoa(int(config.Port)))

		conn, err = grpcDialContext(ctx, dest, opts...)
		if err == nil {
			return conn, err
		}
	}

	return nil, err
}
