// Package grpc is a gRPC-based storage transport.
package grpc

import (
	"encoding/json"
	"errors"
	"net"

	"github.com/tracer/tracer"
	"github.com/tracer/tracer/internal/pbutil"
	"github.com/tracer/tracer/pb"
	"github.com/tracer/tracer/server"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func init() {
	server.RegisterStorageTransport("grpc", setup)
}

func setup(srv *server.Server, conf map[string]interface{}) (server.StorageTransport, error) {
	listen, ok := conf["listen"].(string)
	if !ok {
		return nil, errors.New("missing listen setting for gRPC transport")
	}
	return &GRPC{
		srv:    srv,
		listen: listen,
	}, nil
}

type GRPC struct {
	srv    *server.Server
	listen string
}

// Start implements the server.StorageTransport interface.
func (g *GRPC) Start() error {
	l, err := net.Listen("tcp", g.listen)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	pb.RegisterStorerServer(s, g)
	return s.Serve(l)
}

func (g *GRPC) Store(ctx context.Context, req *pb.StoreRequest) (*pb.StoreResponse, error) {
	for _, span := range req.Spans {
		st, err := pbutil.Timestamp(span.StartTime)
		if err != nil {
			return nil, err
		}
		ft, err := pbutil.Timestamp(span.FinishTime)
		if err != nil {
			return nil, err
		}
		sp := tracer.RawSpan{
			SpanContext: tracer.SpanContext{
				TraceID:  span.TraceId,
				ParentID: span.ParentId,
				SpanID:   span.SpanId,
				Flags:    span.Flags,
			},
			ServiceName:   span.ServiceName,
			OperationName: span.OperationName,
			StartTime:     st,
			FinishTime:    ft,
			Tags:          map[string]interface{}{},
		}
		for _, tag := range span.Tags {
			if tag.Time != nil {
				t, err := pbutil.Timestamp(tag.Time)
				if err != nil {
					return nil, err
				}

				if tag.Key == "logkv-json" {
					rec := tracer.RawLogRecord{
						Timestamp: t,
					}

					var tmp map[string]interface{}
					if err := json.Unmarshal([]byte(tag.Value), &tmp); err != nil {
						return nil, err
					}

					for k, v := range tmp {
						rec.Fields = append(rec.Fields, tracer.RawLogField{k, v})
					}

					sp.Logs = append(sp.Logs, rec)
				} else {
					sp.Logs = append(sp.Logs, tracer.RawLogRecord{
						Timestamp: t,
						Fields: []tracer.RawLogField{
							{"event", tag.Key},
							{"payload", tag.Value},
						},
					})
				}
			} else {
				sp.Tags[tag.Key] = tag.Value
			}
		}

		if err := g.srv.Storage.Store(sp); err != nil {
			return &pb.StoreResponse{}, err
		}
	}
	return &pb.StoreResponse{}, nil
}
