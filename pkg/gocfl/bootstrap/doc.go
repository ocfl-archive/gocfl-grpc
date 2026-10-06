// Package bootstrap provides startup, configuration loading, virtual filesystem (VFS)
// initialization, logging setup, and server lifecycle management for the gocfl gRPC daemon.
//
// It integrates gocfl configuration files (TOML format), extensions (such as Siegfried indexer,
// thumbnail generator, migration, metafile), KeePass/KMS secret resolution for S3 storage,
// and TLS certificate loading.
//
// # Server Initialization Example
//
//	cfg, err := bootstrap.LoadConfig("config.toml")
//	if err != nil {
//	    log.Fatalf("failed to load configuration: %v", err)
//	}
//
//	server, err := bootstrap.NewServer(cfg)
//	if err != nil {
//	    log.Fatalf("failed to create server: %v", err)
//	}
//
//	// Start listening and serving
//	go func() {
//	    if err := server.Serve(); err != nil {
//	        log.Printf("server error: %v", err)
//	    }
//	}()
//
//	// Graceful shutdown on signal
//	server.GracefulStop()
package bootstrap
