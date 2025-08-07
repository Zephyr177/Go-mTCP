// go-mtcp/client/main.go
package main

import (
	"flag"
	"go-mtcp/pipeserver"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	remoteHost := flag.String("remote-host", "127.0.0.1", "Remote MTCP server host")
	remotePort := flag.Int("remote-port", 15201, "Remote MTCP server port")
	listenPort := flag.Int("listen-port", 5201, "Local TCP listening port")
	poolCount := flag.Int("pool", 3, "Number of TCP sub-connections per MTCP socket")
	preLinks := flag.Int("pre-links", 10, "Number of pre-connected MTCP sockets to maintain")
	flag.Parse()

	go pipeserver.CreatePipeServer(
		*remotePort,
		*remoteHost,
		pipeserver.TCP2MTCP,
		*listenPort,
		*poolCount,
		*preLinks,
	)
	
	// Wait for a shutdown signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}
