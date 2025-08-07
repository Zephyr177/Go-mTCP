// go-mtcp/mtcp/mtcp.go
package mtcp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os" // ADDED: For os.ErrDeadlineExceeded
	"sync"
	"sync/atomic"
	"time"
	"unsafe" // ADDED: For atomic CAS on the CID
)

const (
	headerLength  = 4
	maxPacketSize = 65535
)

// MSocket multiplexes a single logical stream over multiple TCP connections.
// It implements the io.ReadWriteCloser interface, including RemoteAddr().
type MSocket struct {
	cid         uint16
	poolCount   int
	conns       []net.Conn
	connsMutex  sync.RWMutex
	readyState  atomic.Value // "opening", "open", "closed"

	// Reading
	packages    map[uint16][]byte
	packagesMux sync.Mutex
	readCh      chan []byte
	readPID     uint32
	readCond    *sync.Cond

	// Writing
	writePID uint32

	// Shutdown
	closeOnce sync.Once
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

// NewMSocket creates a new, uninitialized MSocket.
func NewMSocket(poolCount int) *MSocket {
	ms := &MSocket{
		poolCount: poolCount,
		conns:     make([]net.Conn, 0, poolCount),
		packages:  make(map[uint16][]byte),
		readCh:    make(chan []byte, 1024),
		closeCh:   make(chan struct{}),
	}
	ms.readCond = sync.NewCond(&ms.packagesMux)
	ms.readyState.Store("opening")

	ms.wg.Add(1)
	go ms.reassemble()

	return ms
}

// Connect dials multiple TCP connections to the given address and performs the MTCP handshake.
func (ms *MSocket) Connect(address string, hosts []string) error {
	if len(hosts) == 0 {
		return errors.New("hosts list cannot be empty")
	}

	connectCount := ms.poolCount
	if len(hosts) > 1 {
		connectCount = len(hosts)
	}

	var firstConnErr error
	var firstConnErrMux sync.Mutex
	var successfulConns int32

	var wg sync.WaitGroup
	for i := 0; i < connectCount; i++ {
		wg.Add(1)
		host := hosts[i%len(hosts)]
		go func(i int, h string) {
			defer wg.Done()
			time.Sleep(time.Duration(i*50) * time.Millisecond)

			conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", h, address))
			if err != nil {
				log.Printf("Failed to connect to %s: %v", h, err)
				firstConnErrMux.Lock()
				if firstConnErr == nil {
					firstConnErr = err
				}
				firstConnErrMux.Unlock()
				return
			}

			cidBuf := make([]byte, 2)
			if _, err := io.ReadFull(conn, cidBuf); err != nil {
				log.Printf("Handshake (read cid) failed with %s: %v", h, err)
				conn.Close()
				return
			}
			serverCID := binary.BigEndian.Uint16(cidBuf)

			// Use an atomic Compare-And-Swap to set the master CID only once.
			// The unsafe pointer conversion is a standard pattern for atomic operations on custom types.
			atomic.CompareAndSwapUint32((*uint32)(unsafe.Pointer(&ms.cid)), 0, uint32(serverCID))

			handshakeBuf := make([]byte, 4)
			binary.BigEndian.PutUint16(handshakeBuf[0:2], ms.cid+100)
			binary.BigEndian.PutUint16(handshakeBuf[2:4], ms.cid)
			if _, err := conn.Write(handshakeBuf); err != nil {
				log.Printf("Handshake (write id) failed with %s: %v", h, err)
				conn.Close()
				return
			}

			if atomic.AddInt32(&successfulConns, 1) == 1 {
				ms.readyState.Store("open")
			}
			ms.addConn(conn)

		}(i, host)
	}
	wg.Wait()

	if successfulConns == 0 {
		ms.Close()
		return fmt.Errorf("mtcp: failed to establish any connection: %w", firstConnErr)
	}

	return nil
}

// addConn adds a successfully handshaked net.Conn to the MSocket's pool.
func (ms *MSocket) addConn(conn net.Conn) {
	// CHANGED: Type assert to *net.TCPConn to access TCP-specific methods.
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(10 * time.Second)
		tcpConn.SetNoDelay(true)
	}

	ms.connsMutex.Lock()
	ms.conns = append(ms.conns, conn)
	ms.connsMutex.Unlock()

	ms.wg.Add(1)
	go ms.handleConnRead(conn)
}

// handleConnRead reads packets from a single underlying TCP connection.
func (ms *MSocket) handleConnRead(conn net.Conn) {
	defer ms.wg.Done()
	defer ms.removeConn(conn)

	// CHANGED: removed unused 'header' variable.
	var leftover []byte

	for {
		select {
		case <-ms.closeCh:
			return
		default:
		}

		readBuf := make([]byte, 8192)
		// Set a read deadline to prevent the read from blocking indefinitely.
		// This also allows the closeCh check to be evaluated periodically.
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := conn.Read(readBuf)
		if err != nil {
			// CHANGED: Correctly check for os.ErrDeadlineExceeded
			if err != io.EOF && !errors.Is(err, net.ErrClosed) && !errors.Is(err, os.ErrDeadlineExceeded) {
				log.Printf("Read error on sub-connection: %v", err)
			}
			// If it's a deadline error, we just loop again. Otherwise, the connection is dead.
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				return
			}
		}
		if n == 0 {
			continue
		}

		data := append(leftover, readBuf[:n]...)

		for len(data) >= headerLength {
			length := int(binary.BigEndian.Uint16(data[0:2])) + 1
			end := headerLength + length

			if len(data) < end {
				break
			}

			pid := binary.BigEndian.Uint16(data[2:4])
			packet := make([]byte, length)
			copy(packet, data[headerLength:end])

			ms.packagesMux.Lock()
			ms.packages[pid] = packet
			ms.packagesMux.Unlock()
			ms.readCond.Signal()

			data = data[end:]
		}
		leftover = data
	}
}

// reassemble is a dedicated goroutine that orders packets and pushes them to the read channel.
func (ms *MSocket) reassemble() {
	defer ms.wg.Done()
	defer close(ms.readCh)

	for {
		ms.packagesMux.Lock()
		for {
			select {
			case <-ms.closeCh:
				ms.packagesMux.Unlock()
				return
			default:
			}
			
			currentPID := uint16(ms.readPID) // No need for atomic here, only this goroutine writes to readPID
			packet, ok := ms.packages[currentPID]
			if !ok {
				break
			}

			delete(ms.packages, currentPID)
			ms.readPID++ // Wraps around automatically due to uint16 type

			select {
			case ms.readCh <- packet:
			case <-ms.closeCh:
				ms.packagesMux.Unlock()
				return
			}
		}

		// Wait for a signal that a new packet has arrived.
		// Wait() unlocks the mutex while waiting and re-locks it on wake-up.
		ms.readCond.Wait()
		ms.packagesMux.Unlock()
	}
}

// removeConn removes a connection from the pool and checks if the MSocket is still alive.
func (ms *MSocket) removeConn(conn net.Conn) {
	conn.Close()
	ms.connsMutex.Lock()
	defer ms.connsMutex.Unlock()

	newConns := ms.conns[:0]
	for _, c := range ms.conns {
		if c != conn {
			newConns = append(newConns, c)
		}
	}
	ms.conns = newConns

	if len(ms.conns) == 0 {
		go ms.Close()
	}
}

func (ms *MSocket) Read(p []byte) (n int, err error) {
	data, ok := <-ms.readCh
	if !ok {
		return 0, io.EOF
	}
	n = copy(p, data)
	return n, nil
}

func (ms *MSocket) Write(p []byte) (n int, err error) {
	if ms.readyState.Load() != "open" {
		return 0, errors.New("mtcp: socket is not open")
	}

	totalWritten := 0
	for len(p) > 0 {
		chunkSize := len(p)
		if chunkSize > maxPacketSize {
			chunkSize = maxPacketSize
		}
		chunk := p[:chunkSize]

		pid := uint16(atomic.AddUint32(&ms.writePID, 1) - 1)

		packetBuf := make([]byte, headerLength+len(chunk))
		binary.BigEndian.PutUint16(packetBuf[0:2], uint16(len(chunk)-1))
		binary.BigEndian.PutUint16(packetBuf[2:4], pid)
		copy(packetBuf[headerLength:], chunk)

		ms.connsMutex.RLock()
		if len(ms.conns) == 0 {
			ms.connsMutex.RUnlock()
			return totalWritten, errors.New("mtcp: no available connections to write to")
		}
		conn := ms.conns[int(pid)%len(ms.conns)]
		ms.connsMutex.RUnlock()

		_, err := conn.Write(packetBuf)
		if err != nil {
			log.Printf("Write error on sub-connection: %v", err)
			return totalWritten, err
		}

		totalWritten += len(chunk)
		p = p[chunkSize:]
	}

	return totalWritten, nil
}

func (ms *MSocket) Close() error {
	ms.closeOnce.Do(func() {
		ms.readyState.Store("closed")
		close(ms.closeCh)

		ms.connsMutex.Lock()
		for _, conn := range ms.conns {
			conn.Close()
		}
		ms.conns = nil
		ms.connsMutex.Unlock()

		ms.readCond.Broadcast()
	})
	ms.wg.Wait()
	return nil
}

// RemoteAddr returns the remote network address of the first available connection.
// This makes MSocket conform more closely to the net.Conn interface.
func (ms *MSocket) RemoteAddr() net.Addr {
	ms.connsMutex.RLock()
	defer ms.connsMutex.RUnlock()
	if len(ms.conns) > 0 {
		return ms.conns[0].RemoteAddr()
	}
	return nil
}

// --- MTCP Server Side ---

type MTcpListener struct {
	tcpListener *net.TCPListener
	pending     map[uint16]chan net.Conn
	pendingMux  sync.Mutex
	acceptCh    chan *MSocket
	closeCh     chan struct{}
	wg          sync.WaitGroup
}

func ListenMTcp(port int) (*MTcpListener, error) {
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	tcpListener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}

	l := &MTcpListener{
		tcpListener: tcpListener,
		pending:     make(map[uint16]chan net.Conn),
		acceptCh:    make(chan *MSocket),
		closeCh:     make(chan struct{}),
	}

	l.wg.Add(1)
	go l.run()

	log.Printf("MTCP server listening on :%d", port)
	return l, nil
}

func (l *MTcpListener) run() {
	defer l.wg.Done()
	defer close(l.acceptCh)

	var nextCID uint32 = 1

	for {
		conn, err := l.tcpListener.Accept()
		if err != nil {
			select {
			case <-l.closeCh:
				return
			default:
				log.Printf("Error accepting TCP connection: %v", err)
			}
			continue
		}

		cid := uint16(atomic.AddUint32(&nextCID, 1) - 1)
		if atomic.LoadUint32(&nextCID) > 65000 {
			atomic.StoreUint32(&nextCID, 1)
		}

		l.wg.Add(1)
		go l.handleInitialConn(conn, cid)
	}
}

func (l *MTcpListener) handleInitialConn(conn net.Conn, cid uint16) {
	defer l.wg.Done()

	cidBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(cidBuf, cid)
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(cidBuf); err != nil {
		conn.Close()
		return
	}

	handshakeBuf := make([]byte, 4)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(conn, handshakeBuf); err != nil {
		conn.Close()
		return
	}
	conn.SetReadDeadline(time.Time{}) // Clear deadline

	respCID := binary.BigEndian.Uint16(handshakeBuf[0:2])
	if respCID != cid+100 {
		conn.Close()
		return
	}

	masterID := binary.BigEndian.Uint16(handshakeBuf[2:4])

	l.pendingMux.Lock()
	connChan, exists := l.pending[masterID]
	if !exists {
		ms := NewMSocket(0)
		ms.cid = masterID
		ms.readyState.Store("open")
		
		// Create a channel for subsequent connections for this masterID
		newConnChan := make(chan net.Conn, 16)
		l.pending[masterID] = newConnChan
		l.pendingMux.Unlock() // Unlock before potentially blocking operations

		ms.addConn(conn)

		// Goroutine to add subsequent connections to the newly created MSocket
		l.wg.Add(1)
		go func(m *MSocket, ch chan net.Conn) {
			defer l.wg.Done()
			defer func() {
				l.pendingMux.Lock()
				delete(l.pending, m.cid)
				l.pendingMux.Unlock()
			}()
			for {
				select {
				case newConn := <-ch:
					m.addConn(newConn)
				case <-m.closeCh:
					return
				case <-l.closeCh:
					return
				}
			}
		}(ms, newConnChan)

		// The new MSocket is ready to be accepted
		select {
		case l.acceptCh <- ms:
		case <-l.closeCh:
			ms.Close()
			return
		}

	} else {
		l.pendingMux.Unlock()
		select {
		case connChan <- conn:
		case <-time.After(5 * time.Second):
			conn.Close()
		}
	}
}

func (l *MTcpListener) Accept() (io.ReadWriteCloser, error) {
	ms, ok := <-l.acceptCh
	if !ok {
		return nil, errors.New("mtcp: listener closed")
	}
	return ms, nil
}

func (l *MTcpListener) Close() error {
	l.tcpListener.Close() // This will cause Accept() in run() to error out
	close(l.closeCh)
	l.wg.Wait()
	return nil
}

func (l *MTcpListener) Addr() net.Addr {
	return l.tcpListener.Addr()
}
