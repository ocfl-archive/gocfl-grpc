package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	configutil "github.com/je4/utils/v2/pkg/config"
	"github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/bootstrap"
)

var (
	flagConfigFile        = flag.String("config", "", "location of toml configuration file")
	flagAddr              = flag.String("addr", "", "gRPC server address to listen on (e.g. :50051)")
	flagLogFile           = flag.String("log-file", "", "log output file (default is console)")
	flagLogLevel          = flag.String("log-level", "", "log level (CRITICAL|ERROR|WARNING|NOTICE|INFO|DEBUG)")
	flagAllowAPIShutdown  = flag.Bool("allow-api-shutdown", false, "allow server shutdown via gRPC API")
	flagS3Endpoint        = flag.String("s3-endpoint", "", "endpoint for S3 buckets")
	flagS3AccessKeyID     = flag.String("s3-access-key-id", "", "access key ID for S3 buckets")
	flagS3SecretAccessKey = flag.String("s3-secret-access-key", "", "secret access key for S3 buckets")
	flagS3Region          = flag.String("s3-region", "", "region for S3 access")
)

func main() {
	flag.Parse()

	cfg, err := bootstrap.LoadConfig(*flagConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading configuration: %v\n", err)
		os.Exit(1)
	}

	// Apply CLI flag overrides if provided
	if *flagAddr != "" {
		cfg.Addr = *flagAddr
	}
	if *flagLogFile != "" {
		cfg.Log.File = *flagLogFile
	}
	if *flagLogLevel != "" {
		cfg.Log.Level = *flagLogLevel
	}
	if *flagAllowAPIShutdown {
		cfg.AllowAPIShutdown = true
	}
	if *flagS3Endpoint != "" {
		cfg.S3.Endpoint = configutil.EnvString(*flagS3Endpoint)
	}
	if *flagS3AccessKeyID != "" {
		cfg.S3.AccessKeyID = configutil.EnvString(*flagS3AccessKeyID)
	}
	if *flagS3SecretAccessKey != "" {
		cfg.S3.AccessKey = configutil.EnvString(*flagS3SecretAccessKey)
	}
	if *flagS3Region != "" {
		cfg.S3.Region = configutil.EnvString(*flagS3Region)
	}

	server, err := bootstrap.NewServer(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error initializing server: %v\n", err)
		os.Exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	serverErrChan := make(chan error, 1)
	go func() {
		if err := server.Serve(); err != nil {
			serverErrChan <- err
		} else {
			close(serverErrChan)
		}
	}()

	fmt.Printf("gocfl gRPC server started on %s. Press Ctrl+C to stop.\n", server.Addr())

	select {
	case sig := <-sigChan:
		fmt.Printf("\nreceived signal %v, shutting down gracefully...\n", sig)
		server.GracefulStop()
		fmt.Println("server stopped.")
	case err, ok := <-serverErrChan:
		if ok && err != nil {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		} else {
			fmt.Println("server stopped via shutdown request.")
		}
	case <-server.Done():
		fmt.Println("server stopped.")
	}
}
