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
	mtcpHeaderLength   = 4     // 2 bytes for length, 2 bytes for packet id
	mtcpMaxPayloadSize = 65535 // Max size for a TCP payload in our protocol
	debugLogEnabled    = false // Enable for verbose logging
)

//================================================================================
// MTCP 核心实现 (MSocket)
//================================================================================

// MSocket represents a multiplexed TCP connection.
type MSocket struct {
	io.ReadWriteCloser
	poolCount   int
	remoteAddrs []string
	cid         uint16 // Master connection ID, assigned by the server
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
	readyState  string // "opening", "open", "closed"
}

// mSubConn represents a single underlying TCP connection for an MSocket.
type mSubConn struct {
	net.Conn
	cid uint16 // Sub-connection ID, unique per sub-connection
	mid uint16 // Master connection ID, same for all sub-conns of an MSocket
}

// NewMSocketClient creates a new client-side MSocket.
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

// NewMSocketServer creates a new server-side MSocket.
func NewMSocketServer(mid uint16) *MSocket {
	ctx, cancel := context.WithCancel(context.Background())
	ms := &MSocket{
		cid:        mid,
		isServer:   true,
		packages:   make(map[uint16][]byte),
		readChan:   make(chan []byte, 1024),
		ctx:        ctx,
		cancel:     cancel,
		readyState: "open", // Server-side is considered open immediately
	}
	return ms
}

// Connect establishes the sub-connections for a client MSocket.
// This function is asynchronous and returns immediately.
func (ms *MSocket) Connect() error {
	if ms.isServer {
		return errors.New("Connect() should only be called by a client MSocket")
	}

	if len(ms.remoteAddrs) == 0 {
		return errors.New("no remote address specified")
	}

	// If only one address is given but poolCount > 1, duplicate the address
	targetAddrs := ms.remoteAddrs
	if len(targetAddrs) == 1 && ms.poolCount > 1 {
		for i := 1; i < ms.poolCount; i++ {
			targetAddrs = append(targetAddrs, targetAddrs[0])
		}
	}

	var firstConnOnce sync.Once
	for _, addr := range targetAddrs {
		go ms.connectSub(addr, &firstConnOnce)
	}

	return nil
}

// connectSub handles the connection loop for a single sub-connection.
func (ms *MSocket) connectSub(address string, firstConnOnce *sync.Once) {
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
			log.Printf("main.go:121: [MSocket WARN] Failed to dial sub-connection to %s: %v. Retrying in 5s...", address, err)
			time.Sleep(5 * time.Second)
			continue
		}

		if err := ms.handshakeClient(conn); err != nil {
			log.Printf("[MSocket WARN] Handshake failed for %s: %v. Retrying in 5s...", address, err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		// After the first successful sub-connection, the MSocket is considered "open".
		firstConnOnce.Do(func() {
			ms.readyState = "open"
			if debugLogEnabled {
				log.Printf("[MSocket DEBUG] MSocket (id: %d) is now open.", ms.cid)
			}
		})

		// This sub-connection is now established and handled by its readLoop.
		// If it disconnects, the readLoop will exit, and this `connectSub`
		// function will loop to try and reconnect.
		return
	}
}

// handshakeClient performs the client-side handshake for a new sub-connection.
func (ms *MSocket) handshakeClient(conn net.Conn) error {
	var serverCid uint16
	// 1. Read the unique sub-connection ID from the server
	if err := binary.Read(conn, binary.BigEndian, &serverCid); err != nil {
		return fmt.Errorf("reading server cid: %w", err)
	}
	subConn := &mSubConn{Conn: conn, cid: serverCid}

	ms.connsMutex.Lock()
	// 2. The first sub-connection establishes the master ID for the MSocket
	if ms.cid == 0 {
		ms.cid = serverCid
	}
	subConn.mid = ms.cid
	ms.connsMutex.Unlock()

	// 3. Send back verification info
	buf := make([]byte, 4)
	binary.BigEndian.PutUint16(buf[0:2], subConn.cid+100) // Verification
	binary.BigEndian.PutUint16(buf[2:4], subConn.mid)     // Master ID
	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("writing client info: %w", err)
	}

	if debugLogEnabled {
		log.Printf("[MSocket DEBUG] Client handshake successful for sub-conn %d (master: %d)", subConn.cid, subConn.mid)
	}
	ms.addSubConn(subConn)
	return nil
}

// addSubConn adds a new sub-connection to the MSocket and starts its read loop.
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

// readLoop reads data from a sub-connection and reassembles it in order.
func (ms *MSocket) readLoop(subConn *mSubConn) {
	defer func() {
		subConn.Close()
		ms.connsMutex.Lock()
		
		// BUG修复：在从 ms.conns 移除断开的连接时，必须处理并发场景。
		// 直接遍历并构建一个新的切片是最安全的方式，可以避免在 len(ms.conns) 为0或1时，
		// 计算 newConns 容量 (len-1) 时出现负数，从而引发 panic。
		newConns := make([]*mSubConn, 0, len(ms.conns))
		for _, c := range ms.conns {
			if c != subConn {
				newConns = append(newConns, c)
			}
		}
		ms.conns = newConns

		// If this was the last sub-connection, close the entire MSocket
		if len(ms.conns) == 0 {
			if debugLogEnabled {
				log.Printf("[MSocket DEBUG] All sub-connections for MSocket %d are closed. Closing MSocket.", ms.cid)
			}
			ms.Close()
		}
		ms.connsMutex.Unlock()

		// For clients, try to reconnect this sub-connection
		if !ms.isServer {
			var firstConnOnce sync.Once // Dummy, as MSocket is already open
			go ms.connectSub(subConn.RemoteAddr().String(), &firstConnOnce)
		}
	}()

	reader := bufio.NewReader(subConn)
	header := make([]byte, mtcpHeaderLength)

	for {
		select {
		case <-ms.ctx.Done():
			return
		default:
			// Read header: 2 bytes length, 2 bytes packet ID
			_, err := io.ReadFull(reader, header)
			if err != nil {
				if err != io.EOF && !errors.Is(err, net.ErrClosed) {
					log.Printf("[MSocket WARN] Sub-conn %d (master: %d) read header error: %v", subConn.cid, subConn.mid, err)
				}
				return
			}

			// [FIX] Simplified length handling
			length := binary.BigEndian.Uint16(header[0:2])
			pid := binary.BigEndian.Uint16(header[2:4])

			if length > mtcpMaxPayloadSize {
				log.Printf("[MSocket ERROR] Invalid payload length received: %d", length)
				return
			}
			payload := make([]byte, length)

			// Read payload
			_, err = io.ReadFull(reader, payload)
			if err != nil {
				if err != io.EOF && !errors.Is(err, net.ErrClosed) {
					log.Printf("[MSocket WARN] Sub-conn %d (master: %d) read payload error: %v", subConn.cid, subConn.mid, err)
				}
				return
			}

			// Reassemble packets in order
			ms.readMutex.Lock()
			ms.packages[pid] = payload
			for {
				data, ok := ms.packages[ms.readPid]
				if !ok {
					break // Next packet is not here yet, wait for it
				}
				delete(ms.packages, ms.readPid)
				ms.readChan <- data
				ms.readPid++
			}
			ms.readMutex.Unlock()
		}
	}
}

// Read reads data from the reassembled stream.
func (ms *MSocket) Read(p []byte) (n int, err error) {
	select {
	case <-ms.ctx.Done():
		return 0, io.EOF
	case data, ok := <-ms.readChan:
		if !ok {
			return 0, io.EOF
		}
		n = copy(p, data)
		// Note: If n < len(data), the user's buffer was too small. The rest of the data is lost.
		// This is standard io.Reader behavior.
		return n, nil
	}
}

// Write splits data into chunks and sends them over available sub-connections.
func (ms *MSocket) Write(p []byte) (n int, err error) {
	if ms.readyState != "open" {
		// Wait a moment for the connection to potentially open
		<-time.After(10 * time.Millisecond)
		if ms.readyState != "open" {
			return 0, errors.New("msocket is not open")
		}
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
		// [FIX] Simplified length handling
		binary.BigEndian.PutUint16(header[0:2], uint16(len(chunk)))
		binary.BigEndian.PutUint16(header[2:4], pid)
		packet := append(header, chunk...)

		conn := ms.selectSubConn()
		if conn == nil {
			return totalWritten, errors.New("no available sub-connections to write")
		}

		_, err := conn.Write(packet)
		if err != nil {
			// This sub-connection might be dead. The readLoop will handle its removal.
			return totalWritten, fmt.Errorf("write to sub-conn failed: %w", err)
		}

		totalWritten += len(chunk)
		dataToWrite = dataToWrite[chunkSize:]
	}

	return totalWritten, nil
}

// selectSubConn picks a sub-connection to write to, using round-robin.
func (ms *MSocket) selectSubConn() *mSubConn {
	ms.connsMutex.RLock()
	defer ms.connsMutex.RUnlock()

	if len(ms.conns) == 0 {
		return nil
	}
	// Simple round-robin based on packet ID
	return ms.conns[int(ms.writePid)%len(ms.conns)]
}

// Close gracefully shuts down the MSocket and all its sub-connections.
func (ms *MSocket) Close() error {
	ms.closeOnce.Do(func() {
		ms.readyState = "closed"
		ms.cancel() // Signal all goroutines to stop

		ms.connsMutex.Lock()
		for _, conn := range ms.conns {
			conn.Close()
		}
		ms.conns = nil
		ms.connsMutex.Unlock()

		ms.readMutex.Lock()
		if ms.readChan != nil {
			close(ms.readChan)
			ms.readChan = nil // Prevent writing to closed channel
			ms.packages = nil
		}
		ms.readMutex.Unlock()

		// Wait for all goroutines (like readLoop) to finish
		go ms.wg.Wait()
	})
	return nil
}

//================================================================================
// MTCP 服务端监听器
//================================================================================

// MTCPListener listens for incoming MSockets.
type MTCPListener struct {
	tcpListener *net.TCPListener
	msockets    map[uint16]*MSocket
	msocketsMux sync.Mutex
	acceptChan  chan *MSocket
	ctx         context.Context
	cancel      context.CancelFunc
}

// ListenMTCP creates a new MTCP listener on a given address.
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

// start is the main accept loop for the listener.
func (l *MTCPListener) start() {
	var tempCid uint16 = 1
	log.Printf("MTCP server listening on %s", l.tcpListener.Addr())

	for {
		conn, err := l.tcpListener.Accept()
		if err != nil {
			select {
			case <-l.ctx.Done():
				return // Listener is closed
			default:
				log.Printf("[MTCP Listener] Accept error: %v", err)
			}
			continue
		}

		go l.handleIncomingSubConn(conn, tempCid)
		tempCid++
		if tempCid > 65000 {
			tempCid = 1 // Wrap around
		}
	}
}

// handleIncomingSubConn performs the server-side handshake.
func (l *MTCPListener) handleIncomingSubConn(tcpConn net.Conn, cid uint16) {
	subConn := &mSubConn{Conn: tcpConn, cid: cid}

	// 1. Send the unique sub-connection ID to the client
	if err := binary.Write(tcpConn, binary.BigEndian, subConn.cid); err != nil {
		log.Printf("[MTCP Listener] Failed to send cid to client: %v", err)
		tcpConn.Close()
		return
	}

	// 2. Read verification info from the client
	buf := make([]byte, 4)
	tcpConn.SetReadDeadline(time.Now().Add(10 * time.Second)) // Timeout for handshake
	if _, err := io.ReadFull(tcpConn, buf); err != nil {
		log.Printf("[MTCP Listener] Failed to read client info: %v", err)
		tcpConn.Close()
		return
	}
	tcpConn.SetReadDeadline(time.Time{}) // Clear deadline

	// 3. Verify client response
	remoteCidCheck := binary.BigEndian.Uint16(buf[0:2])
	if remoteCidCheck != subConn.cid+100 {
		log.Printf("[MTCP Listener] Client verification failed. Expected %d, got %d", subConn.cid+100, remoteCidCheck)
		tcpConn.Close()
		return
	}

	mid := binary.BigEndian.Uint16(buf[2:4])
	subConn.mid = mid

	// 4. Find or create the master MSocket
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
			ms.Close()
			return
		}
		// Goroutine to clean up the MSocket from the map when it's closed
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
}

// Accept waits for and returns the next MSocket.
func (l *MTCPListener) Accept() (*MSocket, error) {
	select {
	case <-l.ctx.Done():
		return nil, errors.New("listener closed")
	case ms := <-l.acceptChan:
		return ms, nil
	}
}

// Close closes the listener.
func (l *MTCPListener) Close() error {
	l.cancel()
	return l.tcpListener.Close()
}

// Addr returns the listener's network address.
func (l *MTCPListener) Addr() net.Addr {
	return l.tcpListener.Addr()
}

//================================================================================
// 管道和预连接池
//================================================================================

// pipe connects two ReadWriteClosers, copying data between them until one closes.
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

// PreConnPool manages a pool of pre-established connections.
type PreConnPool struct {
	poolChan    chan io.ReadWriteCloser
	factory     func() (io.ReadWriteCloser, error)
	maxSize     int
	mu          sync.Mutex
	currentSize int // Now correctly represents attempts in-flight + pooled
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewPreConnPool creates and maintains a new connection pool.
func NewPreConnPool(factory func() (io.ReadWriteCloser, error), size int) *PreConnPool {
	if size <= 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &PreConnPool{
		poolChan: make(chan io.ReadWriteCloser, size),
		factory:  factory,
		maxSize:  size,
		ctx:      ctx,
		cancel:   cancel,
	}
	go p.maintain()
	return p
}

// [FIX] Corrected pool maintenance logic
func (p *PreConnPool) maintain() {
	// Initially, try to fill the pool
	for i := 0; i < p.maxSize; i++ {
		p.addConnection()
	}

	// Then, use a ticker to check and refill periodically
	ticker := time.NewTicker(2 * time.Second) // Check less frequently
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.addConnection()
		}
	}
}

// addConnection attempts to add one connection to the pool if needed.
var (
	addConnMu   sync.Mutex  // 防止同一个 PreConnPool 并发重试
	nextRetryAt time.Time   // 记录下一次允许重试的时刻
)

func (p *PreConnPool) addConnection() {
	addConnMu.Lock()
	if time.Now().Before(nextRetryAt) {
		addConnMu.Unlock()
		return // 正在退避中，跳过
	}
	addConnMu.Unlock()

	p.mu.Lock()
	if p.currentSize >= p.maxSize {
		p.mu.Unlock()
		return
	}
	p.currentSize++
	p.mu.Unlock()

	go func() {
		defer func() {
			p.mu.Lock()
			p.currentSize--
			p.mu.Unlock()
		}()

		conn, err := p.factory()
		if err != nil {
			// 指数退避：第一次 1 s，第二次 2 s，...，上限 30 s
			backoff := 1 * time.Second
			addConnMu.Lock()
			if nextRetryAt.IsZero() {
				nextRetryAt = time.Now().Add(backoff)
			} else {
				backoff = time.Until(nextRetryAt) * 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
				nextRetryAt = time.Now().Add(backoff)
			}
			addConnMu.Unlock()
			time.Sleep(backoff)
			return
		}

		// 成功就把退避清零
		addConnMu.Lock()
		nextRetryAt = time.Time{}
		addConnMu.Unlock()

		select {
		case <-p.ctx.Done():
			conn.Close()
		case p.poolChan <- conn:
			if debugLogEnabled {
				log.Printf("[PreConnPool DEBUG] Added a new connection to the pool. Pool chan size: %d", len(p.poolChan))
			}
		default:
			conn.Close()
		}
	}()
}

// Get retrieves a connection from the pool.
func (p *PreConnPool) Get() (io.ReadWriteCloser, error) {
	select {
	case conn := <-p.poolChan:
		return conn, nil
	case <-time.After(3 * time.Second): // Slightly longer timeout
		return nil, errors.New("get connection from pool timed out")
	}
}

// Close shuts down the pool and closes all its connections.
func (p *PreConnPool) Close() {
	p.cancel()
	close(p.poolChan)
	for conn := range p.poolChan {
		conn.Close()
	}
}

// Helper function to wait for an MSocket to become 'open'
func waitForMSocketOpen(ctx context.Context, ms *MSocket) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if ms.readyState == "open" {
				return nil
			}
			if ms.readyState == "closed" {
				return errors.New("msocket closed while waiting to open")
			}
		}
	}
}

// run: 启动管道服务 (已重构)
func run(mode string, listenAddr, remoteAddr string, mPoolCount, preLinkCount int) {
	// Step 1: Create pre-connection pool (if required)
	var pool *PreConnPool
	if preLinkCount > 0 {
		log.Printf("Pre-connection pool enabled with size %d", preLinkCount)
		var factory func() (io.ReadWriteCloser, error)

		if mode == "client" {
			// Client mode: pre-connect MSockets
			factory = func() (io.ReadWriteCloser, error) {
				remotes := strings.Split(remoteAddr, ",")
				ms := NewMSocketClient(remotes, mPoolCount)
				if err := ms.Connect(); err != nil {
					return nil, err
				}
				// [FIX] Wait robustly for the connection to be ready
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				if err := waitForMSocketOpen(ctx, ms); err != nil {
					ms.Close()
					log.Printf("[PreConnPool Factory] MSocket connect timed out or failed: %v", err)
					return nil, err
				}
				return ms, nil
			}
		} else { // server mode
			// Server mode: pre-connect plain TCP connections
			factory = func() (io.ReadWriteCloser, error) {
				
				return net.DialTimeout("tcp", remoteAddr, 10*time.Second)
			}
		}
		pool = NewPreConnPool(factory, preLinkCount)
		if pool != nil {
			defer pool.Close()
		}
	}

	// Step 2: Start the appropriate listener and handling loop
	if mode == "client" {
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

				if pool != nil {
					upstreamConn, err = pool.Get()
					if err != nil {
						log.Printf("main.go:674: Failed to get MSocket from pool: %v. Creating a new one.", err)
					}
				}

				// [FIX] Robust fallback logic
				if upstreamConn == nil {
					remotes := strings.Split(remoteAddr, ",")
					ms := NewMSocketClient(remotes, mPoolCount)
					if err := ms.Connect(); err != nil {
						log.Printf("Failed to initiate MSocket connection: %v", err)
						downConn.Close()
						return
					}
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
					if err := waitForMSocketOpen(ctx, ms); err != nil {
						log.Printf("Failed to establish fallback MSocket connection: %v", err)
						ms.Close()
						downConn.Close()
						return
					}
					upstreamConn = ms
				}

				log.Printf("main.go:693: Piping new TCP connection from %s to MTCP at %s", downConn.RemoteAddr(), remoteAddr)
				pipe(downConn, upstreamConn)
			}(downstreamConn)
		}

	} else { // server mode
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

				if pool != nil {
					upstreamConn, err = pool.Get()
					if err != nil {
						log.Printf("Failed to get TCP conn from pool: %v. Creating a new one.", err)
					}
				}

				if upstreamConn == nil {
					
					upstreamConn, err = net.DialTimeout("tcp", remoteAddr, 10*time.Second)
				}

				if err != nil {
					log.Printf("Failed to establish upstream TCP connection to %s: %v", remoteAddr, err)
					downConn.Close()
					return
				}
				log.Printf("main.go:734: Piping new MTCP connection (ID: %d) to TCP at %s", downConn.cid, remoteAddr)
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
	clientPreLink := clientCmd.Int("pre", 10, "Pre-connection pool size")

	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	serverListen := serverCmd.String("l", "0.0.0.0:15201", "Listening address for MTCP")
	serverRemote := serverCmd.String("r", "127.0.0.1:8080", "Upstream TCP service address")
	serverPreLink := serverCmd.Int("pre", 0, "Pre-connection pool size (for mtcp to tcp)")

	if len(os.Args) < 2 {
		fmt.Println("Usage: program <client|server> [options]")
		fmt.Println("\nClient Options:")
		clientCmd.PrintDefaults()
		fmt.Println("\nServer Options:")
		serverCmd.PrintDefaults()
		return
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds) // Use microseconds for better timing analysis

	switch os.Args[1] {
	case "client":
		clientCmd.Parse(os.Args[2:])
		run("client", *clientListen, *clientRemote, *clientPool, *clientPreLink)
	case "server":
		serverCmd.Parse(os.Args[2:])
		run("server", *serverListen, *serverRemote, 0, *serverPreLink)
	default:
		fmt.Println("Expected 'client' or 'server' subcommands")
		os.Exit(1)
	}
}
