// (c) Copyright IBM Corp. 2021
// (c) Copyright Instana Inc. 2016

// +build go1.10

package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/opentracing/opentracing-go/ext"

	"github.com/instana/go-sensor/example/grpc-example/pb"

	instana "github.com/instana/go-sensor"
	"github.com/instana/go-sensor/instrumentation/instagrpc"
	"google.golang.org/grpc"
)

const (
	port        = ":43210"
	address     = "localhost"
	testMessage = "Hi!"
)

func main() {
	// Initialize server sensor to instrument request handlers
	sensor := instana.NewSensor("grpc-server")

	// To instrument server calls add instagrpc.UnaryServerInterceptor(sensor) and
	// instagrpc.StreamServerInterceptor(sensor) to the list of server options when
	// initializing the server
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(instagrpc.UnaryServerInterceptor(sensor)),
		grpc.StreamInterceptor(instagrpc.StreamServerInterceptor(sensor)),
	)

	pb.RegisterEchoServiceServer(srv, &Service{})

	go startServer(srv)

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		sensor := instana.NewSensor("grpc-client")
		for {
			select {
			case <-ticker.C:
				log.Println("Call server...")
				response := Call(context.Background(), sensor, address+port, testMessage)
				log.Printf("Response << %s", response)
			}
		}
	}()

	select {}
}

// Call send pb.EchoRequest request with "message" to the "address" using generated grpc pb.EchoServiceClient client
// and returns the message from the response
func Call(ctx context.Context, sensor *instana.Sensor, address, message string) string {
	conn, err := grpc.Dial(
		address,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		// To instrument client calls add instagrpc.UnaryClientInterceptor(sensor) and
		// instagrpc.StringClientInterceptor(sensor) to the DialOption list while dialing
		// the GRPC server.
		grpc.WithUnaryInterceptor(instagrpc.UnaryClientInterceptor(sensor)),
		grpc.WithStreamInterceptor(instagrpc.StreamClientInterceptor(sensor)),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	defer conn.Close()

	client := pb.NewEchoServiceClient(conn)

	// The call should always start with an entry span (https://www.instana.com/docs/tracing/custom-best-practices/#start-new-traces-with-entry-spans)
	// Normally this would be your HTTP/GRPC/message queue request span, but here we need to
	// create it explicitly.
	sp := sensor.Tracer().
		StartSpan("client-call").
		SetTag(string(ext.SpanKind), "entry")

	r, err := client.Echo(
		// Create a context that holds the parent entry span and pass it to the GRPC call
		instana.ContextWithSpan(ctx, sp),
		&pb.EchoRequest{Message: message},
	)

	if err != nil {
		log.Fatalf("error while echoing: %v", err)
	}

	return r.Message
}

// Service is used to implement pb.EchoService
type Service struct {
	pb.UnimplementedEchoServiceServer
}

// Echo implements pb.EchoService
func (s *Service) Echo(ctx context.Context, in *pb.EchoRequest) (*pb.EchoReply, error) {
	log.Printf("Request << %s", in.GetMessage())

	// Extract the parent span and use its tracer to initialize any child spans to trace the calls
	// inside the handler, e.g. database queries, 3rd-party API requests, etc.
	if parent, ok := instana.SpanFromContext(ctx); ok {
		sp := parent.Tracer().StartSpan("echo-call")
		defer sp.Finish()
	}

	return &pb.EchoReply{Message: in.GetMessage()}, nil
}

func startServer(srv *grpc.Server) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	log.Println("Starting server...")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
