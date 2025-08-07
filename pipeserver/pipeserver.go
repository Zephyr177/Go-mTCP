// go-mtcp/pipeserver/pipeserver.go
package pipeserver

import (
	"fmt"
	"go-mtcp/mtcp"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// ... (PipeType and Listener interface are unchanged) ...
type PipeType int

const (
	TCP2MTCP PipeType = iota
	MTCP2TCP
)

type Listener interface {
	Accept() (io.ReadWriteCloser, error)
	Close() error
	Addr() net.Addr
}

func CreatePipeServer(upstreamPort int, upstreamHost string, pipeType PipeType, listenPort int, poolCount int, preLinkCount int) {
	var createListener func() (Listener, error)
	var connectUpstream func() (io.ReadWriteCloser, error)

	upstreamHosts := []string{upstreamHost}

	switch pipeType {
	case TCP2MTCP:
		createListener = func() (Listener, error) {
			l, err := net.Listen("tcp", fmt.Sprintf(":%d", listenPort))
			if err != nil {
				return nil, err
			}
			return &tcpListenerAdapter{l.(*net.TCPListener)}, nil // Cast to TCPListener
		}
		connectUpstream = func() (io.ReadWriteCloser, error) {
			ms := mtcp.NewMSocket(poolCount)
			err := ms.Connect(fmt.Sprintf("%d", upstreamPort), upstreamHosts)
			if err != nil {
				ms.Close()
				return nil, err
			}
			return ms, nil
		}
	case MTCP2TCP:
		createListener = func() (Listener, error) {
			return mtcp.ListenMTcp(listenPort)
		}
		connectUpstream = func() (io.ReadWriteCloser, error) {
			return net.Dial("tcp", fmt.Sprintf("%s:%d", upstreamHost, upstreamPort))
		}
	default:
		log.Fatalf("Unknown pipe type")
	}

	preConnPool := make(chan io.ReadWriteCloser, preLinkCount)
	if preLinkCount > 0 {
		log.Printf("Pre-connection pool enabled with size %d", preLinkCount)
		for i := 0; i < preLinkCount; i++ {
			go preConnect(preConnPool, connectUpstream)
		}
	}

	listener, err := createListener()
	if err != nil {
		log.Fatalf("Failed to start listener on port %d: %v", listenPort, err)
	}
	defer listener.Close()

	log.Printf("Server started successfully. Listening on %s", listener.Addr())

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			if ne, ok := err.(net.Error); ok && !ne.Temporary() {
				break
			}
			continue
		}

		go handlePipe(conn, preConnPool, connectUpstream)
	}
}

func preConnect(pool chan io.ReadWriteCloser, connectFn func() (io.ReadWriteCloser, error)) {
	conn, err := connectFn()
	if err != nil {
		log.Printf("Pre-connect failed: %v. Retrying in 5 seconds...", err)
		time.Sleep(5 * time.Second)
		go preConnect(pool, connectFn)
		return
	}
	pool <- conn
}

// CHANGED: Added helper to safely get remote address for logging
func getRemoteAddr(c io.ReadWriteCloser) net.Addr {
	// Type assert to see if the closer implements the net.Conn interface
	if conn, ok := c.(net.Conn); ok {
		return conn.RemoteAddr()
	}
	// Also check for our custom MSocket type
	if ms, ok := c.(*mtcp.MSocket); ok {
		return ms.RemoteAddr()
	}
	return nil
}

func handlePipe(downstream io.ReadWriteCloser, pool chan io.ReadWriteCloser, connectFn func() (io.ReadWriteCloser, error)) {
	defer downstream.Close()

	var upstream io.ReadWriteCloser
	var err error

	select {
	case upstream = <-pool:
		log.Println("Using a pre-connected socket.")
		if len(pool) < cap(pool) { // Only replenish if not full
			go preConnect(pool, connectFn)
		}
	default:
		log.Println("Pool empty, creating a new upstream connection.")
		upstream, err = connectFn()
		if err != nil {
			log.Printf("Failed to connect to upstream: %v", err)
			return
		}
	}
	defer upstream.Close()

	// CHANGED: Use the helper function for safe address logging
	log.Printf("Piping connection: %v <-> %v", getRemoteAddr(downstream), getRemoteAddr(upstream))

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(upstream, downstream)
		upstream.Close()
		downstream.Close()
	}()
	go func() {
		defer wg.Done()
		io.Copy(downstream, upstream)
		downstream.Close()
		upstream.Close()
	}()

	wg.Wait()
	log.Println("Pipe closed.")
}

type tcpListenerAdapter struct {
	*net.TCPListener
}

func (a *tcpListenerAdapter) Accept() (io.ReadWriteCloser, error) {
	return a.AcceptTCP()
}
