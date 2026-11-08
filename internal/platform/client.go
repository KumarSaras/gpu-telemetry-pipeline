package platform

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

func Dial(target string, maxBytes int) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxBytes+1024), grpc.MaxCallSendMsgSize(maxBytes)), grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}))
}
