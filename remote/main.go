// go-mtcp/remote/main.go
package main

import (
	"flag"
	"go-mtcp/pipeserver"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	targetHost := flag.String("target-host", "127.0.0.1", "The final destination host for the tunnel")
	targetPort := flag.Int("target-port", 5201, "The final destination port for the tunnel")
	listenPort := flag.Int("listen-port", 15201, "The MTCP listening port")
	flag.Parse()

	// On the remote side, the pre-connection pool is usually not needed, but we keep the parameter for consistency.
	// You can set it to a non-zero value if you want to pre-connect to the final target service.
	go pipeserver.CreatePipeServer(
		*targetPort,
		*targetHost,
		pipeserver.MTCP2TCP,
		*listenPort,
		0, // Pool count is irrelevant for MTCP listener side
		0, // Pre-links to final target
	)

	// Wait for a shutdown signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}
