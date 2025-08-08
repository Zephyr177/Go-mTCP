package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	mtcpHeaderLength   = 4
	mtcpMaxPayloadSize = 65535
	debugLogEnabled    = false
)

//================================================================================
// MTCP 核心实现 (MSocket)
//================================================================================
type MSocket struct {
	io.ReadWriteCloser
	poolCount   int
	remoteAddrs []string
	cid         uint16
	isServer    bool
	conns       []*mSubConn
	connsMutex  sync.RWMutex
	packages    map[uint16][]byte
	readPid     uint16
	readChan    chan []byte
	readMutex   sync.Mutex
	writePid    uint16
	writeMutex  sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	closeOnce   sync.Once
	wg          sync.WaitGroup
	readyState  string
}

type mSubConn struct {
	net.Conn
	cid uint16
	mid uint16
}

func NewMSocketClient(addrs []string, poolCount int) *MSocket {
	ctx, cancel := context.WithCancel(context.Background())
	ms := &MSocket{
		poolCount:   poolCount,
		remoteAddrs: addrs,
		isServer:    false,
		packages:    make(map[uint16][]byte),
		readChan:    make(chan []byte, 1024),
		ctx:         ctx,
		cancel:      cancel,
		readyState:  "opening",
	}
	return ms
}

func NewMSocketServer(mid uint16) *MSocket {
	ctx, cancel := context.WithCancel(context.Background())
	ms := &MSocket{
		cid:        mid,
		isServer:   true,
		packages:   make(map[uint16][]byte),
		readChan:   make(chan []byte, 1024),
		ctx:        ctx,
		cancel:     cancel,
		readyState: "open",
	}
	return ms
}

// Connect: Made blocking until the first sub-connection is established or a timeout occurs.
func (ms *MSocket) Connect() error {
	if ms.isServer {
		return errors.New("Connect() should only be called by a client MSocket")
	}

	if len(ms.remoteAddrs) > 1 {
		ms.poolCount = len(ms.remoteAddrs)
	} else if len(ms.remoteAddrs) == 0 {
		return errors.New("no remote address specified")
	}

	targetAddrs := ms.remoteAddrs
	if len(targetAddrs) == 1 && ms.poolCount > 1 {
		for i := 1; i < ms.poolCount; i++ {
			targetAddrs = append(targetAddrs, targetAddrs[0])
		}
	}

	var firstConnOnce sync.Once
	connectors := &sync.WaitGroup{}
	connectors.Add(len(targetAddrs))
	successChan := make(chan struct{}, 1) // Channel to signal first successful connection

	for _, addr := range targetAddrs {
		go func(address string) {
			defer connectors.Done()
			for {
				select {
				case <-ms.ctx.Done():
					return
				default:
				}

				if debugLogEnabled {
					log.Printf("[MSocket DEBUG] Dialing sub-connection to %s...", address)
				}
				conn, err := net.DialTimeout("tcp", address, 10*time.Second)
				if err != nil {
					log.Printf("[MSocket WARN] Failed to dial sub-connection to %s: %v. Retrying in 5s...", address, err)
					time.Sleep(5 * time.Second)
					continue
				}

				if err := ms.handshakeClient(conn); err != nil {
					log.Printf("[MSocket WARN] Handshake failed for %s: %v", address, err)
					conn.Close()
					time.Sleep(5 * time.Second)
					continue
				}

				firstConnOnce.Do(func() {
					ms.readyState = "open"
					if debugLogEnabled {
						log.Printf("[MSocket DEBUG] MSocket (id: %d) is now open.", ms.cid)
					}
					close(successChan) // Signal success
				})
				return // This goroutine's job is done
			}
		}(addr)
	}

	// Block until the first connection succeeds or we time out
	select {
	case <-successChan:
		return nil // At least one connection was successful
	case <-time.After(20 * time.Second): // Global timeout for connecting
		ms.Close() // Clean up on timeout
		return errors.New("msocket connect timed out, no sub-connection was established")
	}
}

func (ms *MSocket) handshakeClient(conn net.Conn) error {
	var serverCid uint16
	if err := binary.Read(conn, binary.BigEndian, &serverCid); err != nil {
		return fmt.Errorf("reading server cid: %w", err)
	}
	subConn := &mSubConn{Conn: conn, cid: serverCid}

	ms.connsMutex.Lock()
	if ms.cid == 0 {
		ms.cid = serverCid
	}
	subConn.mid = ms.cid
	ms.connsMutex.Unlock()

	buf := make([]byte, 4)
	binary.BigEndian.PutUint16(buf[0:2], subConn.cid+100)
	binary.BigEndian.PutUint16(buf[2:4], subConn.mid)
	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("writing client info: %w", err)
	}

	if debugLogEnabled {
		log.Printf("[MSocket DEBUG] Client handshake successful for sub-conn %d (master: %d)", subConn.cid, subConn.mid)
	}
	ms.addSubConn(subConn)
	return nil
}

func (ms *MSocket) addSubConn(subConn *mSubConn) {
	ms.connsMutex.Lock()
	ms.conns = append(ms.conns, subConn)
	ms.connsMutex.Unlock()

	ms.wg.Add(1)
	go func() {
		defer ms.wg.Done()
		ms.readLoop(subConn)
	}()
}

func (ms *MSocket) readLoop(subConn *mSubConn) {
	defer func() {
		ms.connsMutex.Lock()
		for i, c := range ms.conns {
			if c == subConn {
				ms.conns = append(ms.conns[:i], ms.conns[i+1:]...)
				break
			}
		}
		if len(ms.conns) == 0 {
			if debugLogEnabled {
				log.Printf("[MSocket DEBUG] All sub-connections for MSocket %d are closed. Closing MSocket.", ms.cid)
			}
			ms.Close()
		}
		ms.connsMutex.Unlock()
		subConn.Close()
	}()

	reader := bufio.NewReader(subConn)
	header := make([]byte, mtcpHeaderLength)

	for {
		select {
		case <-ms.ctx.Done():
			return
		default:
			_, err := io.ReadFull(reader, header)
			if err != nil {
				if err != io.EOF && !errors.Is(err, net.ErrClosed) {
					log.Printf("[MSocket WARN] Sub-conn %d (master: %d) read header error: %v", subConn.cid, subConn.mid, err)
				}
				return
			}
			length := binary.BigEndian.Uint16(header[0:2])
			pid := binary.BigEndian.Uint16(header[2:4])
			payload := make([]byte, length+1)
			_, err = io.ReadFull(reader, payload)
			if err != nil {
				if err != io.EOF && !errors.Is(err, net.ErrClosed) {
					log.Printf("[MSocket WARN] Sub-conn %d (master: %d) read payload error: %v", subConn.cid, subConn.mid, err)
				}
				return
			}
			ms.readMutex.Lock()
			ms.packages[pid] = payload
			for {
				data, ok := ms.packages[ms.readPid]
				if !ok {
					break
				}
				delete(ms.packages, ms.readPid)
				ms.readChan <- data
				ms.readPid++
			}
			ms.readMutex.Unlock()
		}
	}
}

func (ms *MSocket) Read(p []byte) (n int, err error) {
	select {
	case <-ms.ctx.Done():
		return 0, io.EOF
	case data, ok := <-ms.readChan:
		if !ok {
			return 0, io.EOF
		}
		n = copy(p, data)
		if n < len(data) {
			log.Printf("[MSocket WARN] Read buffer is smaller than a single package. Data might be lost.")
		}
		return n, nil
	}
}

func (ms *MSocket) Write(p []byte) (n int, err error) {
	if ms.readyState != "open" {
		return 0, errors.New("msocket is not open")
	}

	ms.writeMutex.Lock()
	defer ms.writeMutex.Unlock()

	totalWritten := 0
	dataToWrite := p

	for len(dataToWrite) > 0 {
		chunkSize := len(dataToWrite)
		if chunkSize > mtcpMaxPayloadSize {
			chunkSize = mtcpMaxPayloadSize
		}
		chunk := dataToWrite[:chunkSize]

		pid := ms.writePid
		ms.writePid++

		header := make([]byte, mtcpHeaderLength)
		binary.BigEndian.PutUint16(header[0:2], uint16(len(chunk)-1))
		binary.BigEndian.PutUint16(header[2:4], pid)
		packet := append(header, chunk...)

		conn := ms.selectSubConn()
		if conn == nil {
			return totalWritten, errors.New("no available sub-connections to write")
		}

		_, err := conn.Write(packet)
		if err != nil {
			return totalWritten, fmt.Errorf("write to sub-conn failed: %w", err)
		}

		totalWritten += len(chunk)
		dataToWrite = dataToWrite[chunkSize:]
	}

	return totalWritten, nil
}

func (ms *MSocket) selectSubConn() *mSubConn {
	ms.connsMutex.RLock()
	defer ms.connsMutex.RUnlock()

	if len(ms.conns) == 0 {
		return nil
	}
	return ms.conns[ms.writePid%uint16(len(ms.conns))]
}

func (ms *MSocket) Close() error {
	ms.closeOnce.Do(func() {
		ms.readyState = "closed"
		ms.cancel()

		ms.connsMutex.Lock()
		for _, conn := range ms.conns {
			conn.Close()
		}
		ms.conns = nil
		ms.connsMutex.Unlock()

		ms.readMutex.Lock()
		if ms.readChan != nil {
			close(ms.readChan)
			ms.packages = nil
		}
		ms.readMutex.Unlock()

		go func() {
			ms.wg.Wait()
		}()
	})
	return nil
}

//================================================================================
// MTCP 服务端监听器
//================================================================================
type MTCPListener struct {
	tcpListener *net.TCPListener
	msockets    map[uint16]*MSocket
	msocketsMux sync.Mutex
	acceptChan  chan *MSocket
	ctx         context.Context
	cancel      context.CancelFunc
}

func ListenMTCP(addr string) (*MTCPListener, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	tcpListener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	listener := &MTCPListener{
		tcpListener: tcpListener,
		msockets:    make(map[uint16]*MSocket),
		acceptChan:  make(chan *MSocket, 128),
		ctx:         ctx,
		cancel:      cancel,
	}

	go listener.start()

	return listener, nil
}

func (l *MTCPListener) start() {
	var tempCid uint16 = 1
	log.Printf("MTCP server listening on %s", l.tcpListener.Addr())

	for {
		conn, err := l.tcpListener.Accept()
		if err != nil {
			select {
			case <-l.ctx.Done():
				return
			default:
				log.Printf("[MTCP Listener] Accept error: %v", err)
			}
			continue
		}

		go func(tcpConn net.Conn) {
			subConn := &mSubConn{Conn: tcpConn, cid: tempCid}
			tempCid++
			if tempCid > 65000 {
				tempCid = 1
			}

			if err := binary.Write(tcpConn, binary.BigEndian, subConn.cid); err != nil {
				log.Printf("[MTCP Listener] Failed to send cid to client: %v", err)
				tcpConn.Close()
				return
			}

			buf := make([]byte, 4)
			if _, err := io.ReadFull(tcpConn, buf); err != nil {
				log.Printf("[MTCP Listener] Failed to read client info: %v", err)
				tcpConn.Close()
				return
			}

			remoteCidCheck := binary.BigEndian.Uint16(buf[0:2])
			if remoteCidCheck != subConn.cid+100 {
				log.Printf("[MTCP Listener] Client verification failed. Expected %d, got %d", subConn.cid+100, remoteCidCheck)
				tcpConn.Close()
				return
			}

			mid := binary.BigEndian.Uint16(buf[2:4])
			subConn.mid = mid

			l.msocketsMux.Lock()
			ms, exists := l.msockets[mid]
			if !exists {
				ms = NewMSocketServer(mid)
				l.msockets[mid] = ms
				select {
				case l.acceptChan <- ms:
					if debugLogEnabled {
						log.Printf("[MTCP Listener DEBUG] New MSocket (id: %d) accepted.", mid)
					}
				case <-l.ctx.Done():
					l.msocketsMux.Unlock()
					return
				}
				go func(id uint16) {
					<-ms.ctx.Done()
					l.msocketsMux.Lock()
					delete(l.msockets, id)
					l.msocketsMux.Unlock()
					if debugLogEnabled {
						log.Printf("[MTCP Listener DEBUG] MSocket (id: %d) closed and removed.", id)
					}
				}(mid)
			}
			l.msocketsMux.Unlock()

			ms.addSubConn(subConn)

		}(conn)
	}
}

func (l *MTCPListener) Accept() (*MSocket, error) {
	select {
	case <-l.ctx.Done():
		return nil, errors.New("listener closed")
	case ms := <-l.acceptChan:
		return ms, nil
	}
}

func (l *MTCPListener) Close() error {
	l.cancel()
	return l.tcpListener.Close()
}

func (l *MTCPListener) Addr() net.Addr {
	return l.tcpListener.Addr()
}

//================================================================================
// 管道工具
//================================================================================
type pipeMode int

const (
	TCP2MTCP pipeMode = iota
	MTCP2TCP pipeMode = iota
)

func pipe(a, b io.ReadWriteCloser) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer a.Close()
		defer b.Close()
		io.Copy(a, b)
	}()
	go func() {
		defer wg.Done()
		defer a.Close()
		defer b.Close()
		io.Copy(b, a)
	}()

	wg.Wait()
}

// run: 启动管道服务 (已移除预连接池)
func run(mode pipeMode, listenAddr, remoteAddr string, mPoolCount int) {
	if mode == TCP2MTCP {
		// 客户端模式: 监听 TCP, 连接到 MTCP
		listener, err := net.Listen("tcp", listenAddr)
		if err != nil {
			log.Fatalf("Failed to listen on %s: %v", listenAddr, err)
		}
		defer listener.Close()
		log.Printf("TCP to MTCP mode. Listening for TCP on %s, forwarding to MTCP at %s", listenAddr, remoteAddr)

		for {
			downstreamConn, err := listener.Accept()
			if err != nil {
				log.Printf("Accept failed: %v", err)
				continue
			}

			go func(downConn net.Conn) {
				var upstreamConn io.ReadWriteCloser
				var err error

				// 动态创建新的 MSocket 连接
				remotes := strings.Split(remoteAddr, ",")
				ms := NewMSocketClient(remotes, mPoolCount)
				if err = ms.Connect(); err == nil {
					upstreamConn = ms
				} else {
					ms.Close()
				}

				if err != nil {
					log.Printf("Failed to establish upstream MSocket connection: %v", err)
					downConn.Close()
					return
				}

				log.Printf("Piping new TCP connection from %s to MTCP at %s", downConn.RemoteAddr(), remoteAddr)
				pipe(downConn, upstreamConn)
			}(downstreamConn)
		}

	} else { // MTCP2TCP
		// 服务端模式: 监听 MTCP, 连接到 TCP
		listener, err := ListenMTCP(listenAddr)
		if err != nil {
			log.Fatalf("Failed to listen for MTCP on %s: %v", listenAddr, err)
		}
		defer listener.Close()
		log.Printf("MTCP to TCP mode. Listening for MTCP on %s, forwarding to TCP at %s", listenAddr, remoteAddr)

		for {
			downstreamConn, err := listener.Accept()
			if err != nil {
				log.Printf("MTCP Accept failed: %v", err)
				continue
			}

			go func(downConn *MSocket) {
				var upstreamConn io.ReadWriteCloser
				var err error

				// 动态创建新的上游 TCP 连接
				upstreamConn, err = net.Dial("tcp", remoteAddr)

				if err != nil {
					log.Printf("Failed to establish upstream TCP connection: %v", err)
					downConn.Close()
					return
				}

				log.Printf("Piping new MTCP connection (ID: %d) to TCP at %s", downConn.cid, remoteAddr)
				pipe(downConn, upstreamConn)
			}(downstreamConn)
		}
	}
}

//================================================================================
// 主函数和命令行解析
//================================================================================
func main() {
	clientCmd := flag.NewFlagSet("client", flag.ExitOnError)
	clientListen := clientCmd.String("l", "127.0.0.1:5201", "Local listening address (TCP)")
	clientRemote := clientCmd.String("r", "8.8.8.8:15201", "Remote MTCP server address. Use comma to aggregate multi-line, e.g., 'ip1:port,ip2:port'")
	clientPool := clientCmd.Int("p", 3, "Sub-connection count for each MTCP link")

	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	serverListen := serverCmd.String("l", "0.0.0.0:15201", "Listening address for MTCP")
	serverRemote := serverCmd.String("r", "127.0.0.1:5201", "Upstream TCP service address")

	if len(os.Args) < 2 {
		fmt.Println("Usage: program <client|server> [options]")
		fmt.Println("\nClient Options:")
		clientCmd.PrintDefaults()
		fmt.Println("\nServer Options:")
		serverCmd.PrintDefaults()
		return
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	switch os.Args[1] {
	case "client":
		clientCmd.Parse(os.Args[2:])
		run(TCP2MTCP, *clientListen, *clientRemote, *clientPool)
	case "server":
		serverCmd.Parse(os.Args[2:])
		run(MTCP2TCP, *serverListen, *serverRemote, 0)
	default:
		fmt.Println("Expected 'client' or 'server' subcommands")
		os.Exit(1)
	}
}
