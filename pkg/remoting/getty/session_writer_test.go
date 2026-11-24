/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package getty

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	getty "github.com/apache/dubbo-getty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"seata.apache.org/seata-go/v2/pkg/protocol/codec"
	"seata.apache.org/seata-go/v2/pkg/protocol/message"
	"seata.apache.org/seata-go/v2/pkg/util/log"
)

type gcAwareTestSession struct {
	getty.Session
	closed       atomic.Bool
	closeOnce    sync.Once
	closeCh      chan struct{}
	gcDone       atomic.Bool
	writeCalls   atomic.Int32
	mu           sync.Mutex
	connTimeout  time.Duration
	timeoutsSeen []time.Duration
	bodiesSeen   []interface{}
	onWrite      func(msg message.RpcMessage, timeout time.Duration) error
}

func newGCAwareTestSession(onWrite func(msg message.RpcMessage, timeout time.Duration) error) *gcAwareTestSession {
	return &gcAwareTestSession{
		closeCh:     make(chan struct{}),
		connTimeout: defaultSessionWriteTimeout,
		onWrite:     onWrite,
	}
}

func (s *gcAwareTestSession) IsClosed() bool {
	return s.closed.Load()
}

func (s *gcAwareTestSession) Close() {
	s.closed.Store(true)
	s.closeOnce.Do(func() {
		close(s.closeCh)
	})
}

func (s *gcAwareTestSession) simulateGC() {
	s.gcDone.Store(true)
}

func (s *gcAwareTestSession) RemoteAddr() string {
	if s.gcDone.Load() {
		panic("nil Connection dereference in RemoteAddr after session.gc()")
	}
	return "127.0.0.1:8091"
}

func (s *gcAwareTestSession) Stat() string {
	if s.gcDone.Load() {
		return ""
	}
	return "gc-aware-session(127.0.0.1:8091)"
}

func (s *gcAwareTestSession) WriteTimeout() time.Duration {
	if s.gcDone.Load() {
		panic("nil Connection dereference in WriteTimeout after session.gc()")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connTimeout
}

func (s *gcAwareTestSession) SetWriteTimeout(d time.Duration) {
	if s.gcDone.Load() {
		panic("nil Connection dereference in SetWriteTimeout after session.gc()")
	}
	if d < 1 {
		panic("@wTimeout < 1")
	}
	s.mu.Lock()
	s.connTimeout = d
	s.mu.Unlock()
}

func (s *gcAwareTestSession) WritePkg(pkg interface{}, timeout time.Duration) (int, int, error) {
	if s.gcDone.Load() {
		panic("WritePkg called concurrently with or after session.gc()")
	}
	s.writeCalls.Add(1)
	s.mu.Lock()
	if timeout > 0 {
		s.connTimeout = timeout
	}
	effectiveTimeout := s.connTimeout
	s.timeoutsSeen = append(s.timeoutsSeen, effectiveTimeout)
	if rpcMsg, ok := pkg.(message.RpcMessage); ok {
		s.bodiesSeen = append(s.bodiesSeen, rpcMsg.Body)
	}
	fn := s.onWrite
	s.mu.Unlock()

	if rpcMsg, ok := pkg.(message.RpcMessage); ok && fn != nil {
		if err := fn(rpcMsg, effectiveTimeout); err != nil {
			return 0, 0, err
		}
	}
	return 0, 0, nil
}

func TestSessionWriterLockWaitRespectsDeadlineAndSkipsExpiredWrite(t *testing.T) {
	client := newTestClient()

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var callIdx atomic.Int32

	sess := newGCAwareTestSession(func(msg message.RpcMessage, _ time.Duration) error {
		if callIdx.Add(1) == 1 {
			close(firstEntered)
			<-releaseFirst
		}
		client.NotifyRpcMessageResponse(message.RpcMessage{
			ID:   msg.ID,
			Type: message.RequestTypeResponse,
			Body: "ok",
		})
		return nil
	})
	registerSessionWriter(sess, defaultSessionWriteTimeout)
	t.Cleanup(func() { closeSessionWriter(sess) })

	firstDone := make(chan error, 1)
	go func() {
		ctxA, cancelA := context.WithTimeout(WithSession(context.Background(), sess), time.Second)
		defer cancelA()
		_, err := client.SendSyncRequestInContext(ctxA, "req-A")
		firstDone <- err
	}()

	<-firstEntered

	// Request B queues behind Request A on the same session and expires before A finishes writing.
	ctxB, cancelB := context.WithTimeout(WithSession(context.Background(), sess), 25*time.Millisecond)
	defer cancelB()
	startB := time.Now()
	_, errB := client.SendSyncRequestInContext(ctxB, "req-B")
	elapsedB := time.Since(startB)

	require.ErrorIs(t, errB, context.DeadlineExceeded)
	assert.Less(t, elapsedB, 250*time.Millisecond)

	// Now unblock Request A and verify Request B never wrote to the wire or leaked a future.
	close(releaseFirst)
	require.NoError(t, <-firstDone)

	assert.Equal(t, int32(1), sess.writeCalls.Load())
	sess.mu.Lock()
	assert.Equal(t, []interface{}{"req-A"}, sess.bodiesSeen)
	sess.mu.Unlock()
	assert.Equal(t, 0, countFutures(client.gettyRemoting))
}

func TestSessionWriterUnifiesTMAndRMDeadlinesAndProtectsOnCloseGC(t *testing.T) {
	client := GetGettyRemotingClient()

	writeEntered := make(chan struct{})
	allowWriteReturn := make(chan struct{})
	var firstWriteOnce sync.Once

	sess := newGCAwareTestSession(func(msg message.RpcMessage, _ time.Duration) error {
		if msg.Type == message.RequestTypeRequestSync {
			firstWriteOnce.Do(func() {
				close(writeEntered)
				<-allowWriteReturn
			})
			client.NotifyRpcMessageResponse(message.RpcMessage{
				ID:   msg.ID,
				Type: message.RequestTypeResponse,
				Body: "tm-ok",
			})
		}
		return nil
	})
	registerSessionWriter(sess, defaultSessionWriteTimeout)

	// 1. Short-deadline TM request sets <= 80ms timeout, then RM async response and heartbeat
	// restore defaultSessionWriteTimeout (5s) under the same per-session serialization.
	close(allowWriteReturn)
	tmCtx, cancelTM := context.WithTimeout(WithSession(context.Background(), sess), 80*time.Millisecond)
	res, err := client.SendSyncRequestInContext(tmCtx, "tm-short")
	cancelTM()
	require.NoError(t, err)
	assert.Equal(t, "tm-ok", res)

	rmCtx := WithSession(context.Background(), sess)
	require.NoError(t, client.SendAsyncResponseInContext(rmCtx, 77, "rm-phase2-resp"))
	GetGettyClientHandlerInstance().OnCron(sess)

	sess.mu.Lock()
	require.Len(t, sess.timeoutsSeen, 3)
	assert.Greater(t, sess.timeoutsSeen[0], time.Duration(0))
	assert.LessOrEqual(t, sess.timeoutsSeen[0], 80*time.Millisecond)
	assert.Equal(t, defaultSessionWriteTimeout, sess.timeoutsSeen[1])
	assert.Equal(t, defaultSessionWriteTimeout, sess.timeoutsSeen[2])
	sess.mu.Unlock()

	// 2. OnClose blocks until an in-flight WritePkg finishes before returning to session.gc(),
	// and subsequent writes after OnClose+gc return "session is closed" without panicking.
	inFlightEntered := make(chan struct{})
	unblockInFlight := make(chan struct{})
	sess2 := newGCAwareTestSession(func(msg message.RpcMessage, _ time.Duration) error {
		close(inFlightEntered)
		<-unblockInFlight
		client.NotifyRpcMessageResponse(message.RpcMessage{
			ID:   msg.ID,
			Type: message.RequestTypeResponse,
			Body: "done-before-gc",
		})
		return nil
	})
	registerSessionWriter(sess2, defaultSessionWriteTimeout)

	inFlightErrCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(WithSession(context.Background(), sess2), time.Second)
		defer cancel()
		_, sendErr := client.SendSyncRequestInContext(ctx, "in-flight")
		inFlightErrCh <- sendErr
	}()

	<-inFlightEntered

	gcFinished := make(chan struct{})
	go func() {
		// Simulate dubbo-getty handlePackage defer: s.stop() -> OnClose(s) -> s.gc()
		sess2.Close()
		GetGettyClientHandlerInstance().OnClose(sess2)
		sess2.simulateGC()
		close(gcFinished)
	}()

	// OnClose must NOT return to simulateGC while WritePkg is still executing.
	select {
	case <-gcFinished:
		t.Fatal("OnClose returned and ran session.gc() while WritePkg was still in flight")
	case <-time.After(20 * time.Millisecond):
	}

	// Queue a concurrent writer BEFORE Close()/OnClose() finishes so it enters writeSessionPkg
	// right across the OnClose -> simulateGC() transition.
	racingPostGCErrCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(WithSession(context.Background(), sess2), time.Second)
		defer cancel()
		_, sendErr := client.SendSyncRequestInContext(ctx, "racing-across-gc")
		racingPostGCErrCh <- sendErr
	}()

	close(unblockInFlight)
	require.NoError(t, <-inFlightErrCh)
	<-gcFinished
	require.EqualError(t, <-racingPostGCErrCh, "session is closed")

	postGCErrCtx, cancelPost := context.WithTimeout(WithSession(context.Background(), sess2), 50*time.Millisecond)
	defer cancelPost()
	_, postErr := client.SendSyncRequestInContext(postGCErrCtx, "after-gc")
	require.EqualError(t, postErr, "session is closed")
}

func TestReadySessionsReturnsImmediatelyWhenEmpty(t *testing.T) {
	previous := sessionManager
	sessionManager = &SessionManager{}
	t.Cleanup(func() { sessionManager = previous })

	start := time.Now()
	got := GetGettyRemotingClient().ReadySessions()
	assert.Empty(t, got)
	assert.Less(t, time.Since(start), 50*time.Millisecond)
}

func TestSessionWriterInFlightWriteAbortsOnContextCancelAndRedactsApplicationData(t *testing.T) {
	client := newTestClient()

	writeEntered := make(chan struct{})
	var sess *gcAwareTestSession
	sess = newGCAwareTestSession(func(_ message.RpcMessage, _ time.Duration) error {
		close(writeEntered)
		<-sess.closeCh
		return getty.ErrSessionClosed
	})
	registerSessionWriter(sess, defaultSessionWriteTimeout)

	ctx, cancel := context.WithCancel(WithSession(context.Background(), sess))
	go func() {
		<-writeEntered
		time.Sleep(15 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	req := message.BranchRegisterRequest{
		Xid:             "127.0.0.1:8091:991",
		ResourceId:      "order-v2.Create",
		ApplicationData: []byte(`{"actionContext":{"order_id":"SECRET-ORDER-999"}}`),
	}
	_, err := client.SendSyncRequestInContext(ctx, req)
	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), 250*time.Millisecond)
	assert.True(t, sess.IsClosed())
	assert.Equal(t, 0, countFutures(client.gettyRemoting))

	summary := summarizeRpcMessage(message.RpcMessage{ID: 7, Type: message.RequestTypeRequestSync, Body: req})
	assert.Contains(t, summary, "127.0.0.1:8091:991")
	assert.Contains(t, summary, "order-v2.Create")
	assert.NotContains(t, summary, "SECRET-ORDER-999")
	assert.NotContains(t, summary, "actionContext")
}

type capturingTestLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *capturingTestLogger) record(msg string) {
	l.mu.Lock()
	l.lines = append(l.lines, msg)
	l.mu.Unlock()
}

func (l *capturingTestLogger) joined() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func (l *capturingTestLogger) Debug(v ...any)                 { l.record(fmt.Sprint(v...)) }
func (l *capturingTestLogger) Debugf(f string, v ...any)      { l.record(fmt.Sprintf(f, v...)) }
func (l *capturingTestLogger) Info(v ...any)                  { l.record(fmt.Sprint(v...)) }
func (l *capturingTestLogger) Infof(f string, v ...any)       { l.record(fmt.Sprintf(f, v...)) }
func (l *capturingTestLogger) Warn(v ...any)                  { l.record(fmt.Sprint(v...)) }
func (l *capturingTestLogger) Warnf(f string, v ...any)       { l.record(fmt.Sprintf(f, v...)) }
func (l *capturingTestLogger) Error(v ...any)                 { l.record(fmt.Sprint(v...)) }
func (l *capturingTestLogger) Errorf(f string, v ...any)      { l.record(fmt.Sprintf(f, v...)) }
func (l *capturingTestLogger) Panic(v ...any)                 { l.record(fmt.Sprint(v...)) }
func (l *capturingTestLogger) Panicf(f string, v ...any)      { l.record(fmt.Sprintf(f, v...)) }
func (l *capturingTestLogger) Fatal(v ...any)                 { l.record(fmt.Sprint(v...)) }
func (l *capturingTestLogger) Fatalf(f string, v ...any)      { l.record(fmt.Sprintf(f, v...)) }

type gettyConnBackedSession struct {
	getty.Session
	conn     net.Conn
	stopOnce sync.Once
	done     chan struct{}
	wTimeout atomic.Int64
	attrMu   sync.RWMutex
	attrs    map[any]any
}

func newGettyConnBackedSession(conn net.Conn, wTimeout time.Duration) *gettyConnBackedSession {
	s := &gettyConnBackedSession{
		conn:  conn,
		done:  make(chan struct{}),
		attrs: make(map[any]any),
	}
	s.wTimeout.Store(int64(wTimeout))
	return s
}

func (s *gettyConnBackedSession) GetAttribute(key any) any {
	s.attrMu.RLock()
	defer s.attrMu.RUnlock()
	return s.attrs[key]
}

func (s *gettyConnBackedSession) SetAttribute(key, value any) {
	s.attrMu.Lock()
	defer s.attrMu.Unlock()
	s.attrs[key] = value
}

func (s *gettyConnBackedSession) Conn() net.Conn {
	return s.conn
}

func (s *gettyConnBackedSession) RemoteAddr() string {
	return "127.0.0.1:8091"
}

func (s *gettyConnBackedSession) Stat() string {
	// Reproduce dubbo-getty v1.5.0 session.Stat() reading s.Connection without a lock.
	if s.conn == nil {
		return ""
	}
	return "getty-conn-backed-session(127.0.0.1:8091)"
}

func (s *gettyConnBackedSession) IsClosed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *gettyConnBackedSession) WriteTimeout() time.Duration {
	return time.Duration(s.wTimeout.Load())
}

func (s *gettyConnBackedSession) SetWriteTimeout(d time.Duration) {
	if d < 1 {
		panic("@wTimeout < 1")
	}
	s.wTimeout.Store(int64(d))
}

// Close faithfully reproduces dubbo-getty v1.5.0 session.stop() (session.go:849-869):
// it pushes conn's write deadline forward to now.Add(s.WriteTimeout()) and closes s.done
// without closing conn itself (conn is only closed later in session.gc()).
func (s *gettyConnBackedSession) Close() {
	select {
	case <-s.done:
		return
	default:
		s.stopOnce.Do(func() {
			now := time.Now()
			if conn := s.Conn(); conn != nil {
				_ = conn.SetWriteDeadline(now.Add(s.WriteTimeout()))
			}
			close(s.done)
		})
	}
}

// WriteBytes faithfully reproduces dubbo-getty v1.5.0 session.WriteBytes + gettyTCPConn.Send
// (session.go:420-453, connection.go:287-331, where t.conn.SetWriteDeadline is only set when t.compress == CompressNone).
func (s *gettyConnBackedSession) WriteBytes(pkg []byte) (int, error) {
	if s.IsClosed() {
		return 0, getty.ErrSessionClosed
	}
	if !isCompressEncodingEnabled() {
		if wt := s.WriteTimeout(); wt > 0 {
			if err := s.conn.SetWriteDeadline(time.Now().Add(wt)); err != nil {
				return 0, err
			}
		}
	}
	return s.conn.Write(pkg)
}

func (s *gettyConnBackedSession) WritePkg(pkg any, timeout time.Duration) (int, int, error) {
	// If WritePkg were ever called instead of sendSessionFrame's WriteBytes path, log pkg:%#v
	// the exact same way dubbo-getty session.go:413 does so the redaction assertion would catch it.
	pkgBytes, err := rpcPkgHandler.Write(s, pkg)
	if err != nil {
		return 0, 0, err
	}
	if timeout > 0 {
		s.SetWriteTimeout(timeout)
	}
	n, sendErr := s.WriteBytes(pkgBytes)
	if sendErr != nil {
		log.GetLogger().Warnf("%s, [session.WritePkg] @s.Connection.Write(pkg:%#v) = err:%+v", s.Stat(), pkgBytes, sendErr)
		return len(pkgBytes), n, sendErr
	}
	return len(pkgBytes), n, nil
}

func canDialLoopbackTCP() bool {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return false
	}
	defer func() { _ = ln.Close() }()
	c, dialErr := net.DialTimeout("tcp", ln.Addr().String(), 100*time.Millisecond)
	if dialErr != nil {
		return false
	}
	_ = c.Close()
	return true
}

func TestSessionWriterRealTCPBackpressureCancelAbortsWriteAndRedactsLogs(t *testing.T) {
	codec.Init()
	log.Init()
	origLogger := log.GetLogger()
	capLogger := &capturingTestLogger{}
	log.SetLogger(capLogger)
	t.Cleanup(func() {
		log.SetLogger(origLogger)
	})

	const secretPayload = "TOP-SECRET-REAL-TCP-ORDER-8888"
	req := message.BranchRegisterRequest{
		Xid:             "127.0.0.1:8091:992",
		ResourceId:      "order-v2.RealTCPBackpressure",
		ApplicationData: []byte(`{"actionContext":{"order_id":"` + secretPayload + `"}}`),
	}

	// 1. Verify against a real net.Conn (net.Pipe) with dubbo-getty's exact session.stop() Close() semantics
	// (where Close() resets SetWriteDeadline(now + WriteTimeout) and does not close net.Conn):
	clientConn, serverConn := net.Pipe()
	defer func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	}()
	pipeSess := newGettyConnBackedSession(clientConn, defaultSessionWriteTimeout)
	registerSessionWriter(pipeSess, defaultSessionWriteTimeout)

	client := newTestClient()
	ctxPipe, cancelPipe := context.WithCancel(WithSession(context.Background(), pipeSess))
	go func() {
		time.Sleep(25 * time.Millisecond)
		cancelPipe()
	}()
	startPipe := time.Now()
	_, pipeErr := client.SendSyncRequestInContext(ctxPipe, req)
	elapsedPipe := time.Since(startPipe)
	require.ErrorIs(t, pipeErr, context.Canceled)
	assert.Less(t, elapsedPipe, 500*time.Millisecond, "cancel must override session.Close() +5s deadline and abort write")
	assert.True(t, pipeSess.IsClosed())
	_, postPipeWriteErr := clientConn.Write([]byte("x"))
	require.Error(t, postPipeWriteErr, "underlying net.Conn write deadline must remain expired after cancel")
	assert.NotContains(t, capLogger.joined(), secretPayload)

	// 2. When TCP loopback dialing is permitted by the OS environment (e.g. CI and non-sandboxed runs),
	// also verify against a real *net.TCPConn under kernel send-buffer backpressure.
	if !canDialLoopbackTCP() {
		return
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	acceptedDone := make(chan net.Conn, 1)
	go func() {
		srvConn, acceptErr := ln.Accept()
		if acceptErr == nil {
			if tcpSrv, ok := srvConn.(*net.TCPConn); ok {
				_ = tcpSrv.SetReadBuffer(1024)
			}
			acceptedDone <- srvConn
		}
	}()

	rawConn, dialErr := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	require.NoError(t, dialErr)
	defer func() { _ = rawConn.Close() }()
	if tcpConn, ok := rawConn.(*net.TCPConn); ok {
		_ = tcpConn.SetWriteBuffer(1024)
	}

	srvConn := <-acceptedDone
	defer func() { _ = srvConn.Close() }()

	realSess := newGettyConnBackedSession(rawConn, defaultSessionWriteTimeout)
	registerSessionWriter(realSess, defaultSessionWriteTimeout)

	filler := make([]byte, 32*1024)
	for i := 0; i < 256; i++ {
		_ = rawConn.SetWriteDeadline(time.Now().Add(5 * time.Millisecond))
		if _, wErr := rawConn.Write(filler); wErr != nil {
			break
		}
	}
	_ = rawConn.SetWriteDeadline(time.Time{})

	ctx, cancel := context.WithCancel(WithSession(context.Background(), realSess))
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, sendErr := client.SendSyncRequestInContext(ctx, req)
	elapsed := time.Since(start)

	require.ErrorIs(t, sendErr, context.Canceled)
	assert.Less(t, elapsed, 500*time.Millisecond, "cancel must abort in-flight TCP write without waiting for 5s WriteTimeout")
	assert.True(t, realSess.IsClosed())
	_, postWriteErr := rawConn.Write([]byte("x"))
	require.Error(t, postWriteErr)
	assert.NotContains(t, capLogger.joined(), secretPayload)
}

func TestSessionWriterCancelDuringEncodeNeverResetsDeadlineOrWrites(t *testing.T) {
	codec.Init()
	log.Init()

	clientConn, serverConn := net.Pipe()
	defer func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	}()

	// If Write were called after encode, serverReadDone would receive bytes because serverConn is actively reading.
	serverReadBytes := make(chan int, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := serverConn.Read(buf)
		serverReadBytes <- n
	}()

	pipeSess := newGettyConnBackedSession(clientConn, defaultSessionWriteTimeout)
	registerSessionWriter(pipeSess, defaultSessionWriteTimeout)

	ctx, cancel := context.WithCancel(WithSession(context.Background(), pipeSess))
	defer cancel()

	origEncode := encodeSessionPkg
	t.Cleanup(func() {
		encodeSessionPkg = origEncode
	})
	encodeSessionPkg = func(session getty.Session, msg message.RpcMessage) ([]byte, error) {
		pkgBytes, err := origEncode(session, msg)
		// Trigger context cancellation while encoding is in progress, and wait briefly so the AfterFunc
		// callback runs before sendSessionFrame proceeds to the deadline/write step.
		cancel()
		time.Sleep(15 * time.Millisecond)
		return pkgBytes, err
	}

	client := newTestClient()
	req := message.BranchRegisterRequest{
		Xid:             "127.0.0.1:8091:993",
		ResourceId:      "order-v2.CancelDuringEncode",
		ApplicationData: []byte(`{"actionContext":{"order_id":"NO-LATE-WRITE"}}`),
	}
	_, err := client.SendSyncRequestInContext(ctx, req)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, pipeSess.IsClosed(), "canceling before socket Write starts must NOT close the shared session")
	assert.Equal(t, 0, countFutures(client.gettyRemoting))

	select {
	case n := <-serverReadBytes:
		t.Fatalf("expected zero bytes written to net.Conn when canceled during encode, got %d bytes", n)
	case <-time.After(30 * time.Millisecond):
	}

	// Restore encodeSessionPkg and verify the shared session is still healthy and writable for subsequent requests.
	encodeSessionPkg = origEncode
	nWritten, postWriteErr := clientConn.Write([]byte("ok"))
	require.NoError(t, postWriteErr, "shared session net.Conn must remain writable after pre-write cancel")
	assert.Equal(t, 2, nWritten)
	assert.Equal(t, 2, <-serverReadBytes)
}

func TestSessionWriterPostWriteContextCancelKeepsSharedSessionOpenAndCleansFuture(t *testing.T) {
	codec.Init()
	log.Init()

	clientConn, serverConn := net.Pipe()
	defer func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	}()

	pipeSess := newGettyConnBackedSession(clientConn, defaultSessionWriteTimeout)
	registerSessionWriter(pipeSess, defaultSessionWriteTimeout)

	ctx, cancel := context.WithCancel(WithSession(context.Background(), pipeSess))
	defer cancel()

	// Drain the full frame on serverConn so netConn.Write succeeds completely (writeErr == nil),
	// then immediately cancel ctx while SendSyncRequestInContext is waiting for the RPC response.
	frameReceived := make(chan int, 1)
	go func() {
		buf := make([]byte, 4096)
		n, readErr := serverConn.Read(buf)
		if readErr == nil && n > 0 {
			frameReceived <- n
			cancel()
		}
	}()

	client := newTestClient()
	req := message.BranchRegisterRequest{
		Xid:        "127.0.0.1:8091:993b",
		ResourceId: "order-v2.CancelAfterFullWrite",
	}
	_, err := client.SendSyncRequestInContext(ctx, req)
	require.ErrorIs(t, err, context.Canceled)
	assert.Greater(t, <-frameReceived, 0)
	assert.False(t, pipeSess.IsClosed(), "full write followed by ctx cancel must NOT tear down shared session")
	assert.Equal(t, 0, countFutures(client.gettyRemoting))

	// Verify subsequent writes on the shared session still succeed.
	doneRead := make(chan int, 1)
	go func() {
		buf := make([]byte, 64)
		n, _ := serverConn.Read(buf)
		doneRead <- n
	}()
	_, secondWriteErr := clientConn.Write([]byte("next"))
	require.NoError(t, secondWriteErr)
	assert.Equal(t, 4, <-doneRead)

	// Also verify CompressEncoding=true path (session.WriteBytes) preserves shared session on
	// both pre-write (during encode) and post-write context cancellation.
	prevMgr := sessionManager
	sessionManager = newTestSessionManager(nil)
	sessionManager.gettyConf.SessionConfig.CompressEncoding = true
	t.Cleanup(func() {
		sessionManager = prevMgr
	})

	origEncode := encodeSessionPkg
	t.Cleanup(func() {
		encodeSessionPkg = origEncode
	})
	ctxEnc, cancelEnc := context.WithCancel(WithSession(context.Background(), pipeSess))
	encodeSessionPkg = func(session getty.Session, msg message.RpcMessage) ([]byte, error) {
		pkgBytes, encErr := origEncode(session, msg)
		cancelEnc()
		return pkgBytes, encErr
	}
	_, encCancelErr := client.SendSyncRequestInContext(ctxEnc, req)
	require.ErrorIs(t, encCancelErr, context.Canceled)
	assert.False(t, pipeSess.IsClosed(), "CompressEncoding=true pre-write cancel during encode must NOT close shared session")
	encodeSessionPkg = origEncode

	ctxCompPost, cancelCompPost := context.WithCancel(WithSession(context.Background(), pipeSess))
	defer cancelCompPost()
	compFrameRead := make(chan int, 1)
	go func() {
		buf := make([]byte, 4096)
		n, readErr := serverConn.Read(buf)
		if readErr == nil && n > 0 {
			compFrameRead <- n
			cancelCompPost()
		}
	}()
	_, compPostErr := client.SendSyncRequestInContext(ctxCompPost, req)
	require.ErrorIs(t, compPostErr, context.Canceled)
	assert.Greater(t, <-compFrameRead, 0)
	assert.False(t, pipeSess.IsClosed(), "CompressEncoding=true post-write cancel must NOT close shared session")
}

func TestSessionWriterFailureCleanupUsesLockedConnWithoutRacingOnCloseGC(t *testing.T) {
	codec.Init()
	log.Init()

	clientConn, serverConn := net.Pipe()
	defer func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	}()

	pipeSess := newGettyConnBackedSession(clientConn, defaultSessionWriteTimeout)
	registerSessionWriter(pipeSess, defaultSessionWriteTimeout)

	ctx, cancel := context.WithCancel(WithSession(context.Background(), pipeSess))
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	// Concurrently simulate dubbo-getty's OnClose + session.gc() zeroing s.conn immediately after closeSessionWriter.
	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)
		<-ctx.Done()
		pipeSess.Close()
		closeSessionWriter(pipeSess)
		// Reproduce dubbo-getty session.gc() setting s.Connection = nil after OnClose returns.
		pipeSess.conn = nil
	}()

	client := newTestClient()
	_, err := client.SendSyncRequestInContext(ctx, message.BranchRegisterRequest{
		Xid:        "127.0.0.1:8091:994",
		ResourceId: "order-v2.OnCloseGCRace",
	})
	<-gcDone
	require.ErrorIs(t, err, context.Canceled)
	_, postWriteErr := clientConn.Write([]byte("x"))
	require.Error(t, postWriteErr, "captured netConn must have expired write deadline even after session.gc() nils session.Conn()")

	// Also verify the Phase 2 SendAsyncResponseInContext path (closeOnWriteErr=false) when the
	// peer disconnects mid-write and OnClose + session.gc() concurrently nils session.conn:
	// logging the failure must use the lock-captured peerAddr instead of calling session.Stat() after unlock.
	clientConn2, serverConn2 := net.Pipe()
	defer func() {
		_ = clientConn2.Close()
		_ = serverConn2.Close()
	}()
	pipeSess2 := newGettyConnBackedSession(clientConn2, defaultSessionWriteTimeout)
	registerSessionWriter(pipeSess2, defaultSessionWriteTimeout)

	gcDone2 := make(chan struct{})
	go func() {
		defer close(gcDone2)
		time.Sleep(20 * time.Millisecond)
		_ = clientConn2.SetWriteDeadline(time.Unix(1, 0))
		pipeSess2.Close()
		closeSessionWriter(pipeSess2)
		pipeSess2.conn = nil
	}()

	asyncErr := client.SendAsyncResponseInContext(
		WithSession(context.Background(), pipeSess2),
		77,
		message.BranchCommitResponse{},
	)
	<-gcDone2
	require.Error(t, asyncErr)

	// 3. Also verify OnCron + TM handshake label logging against concurrent OnClose + session.gc().
	clientConn3, serverConn3 := net.Pipe()
	defer func() {
		_ = clientConn3.Close()
		_ = serverConn3.Close()
	}()
	pipeSess3 := newGettyConnBackedSession(clientConn3, defaultSessionWriteTimeout)
	pipeSess3.SetAttribute(heartBeatRetryTimesKey, maxHeartBeatRetryTimes)
	registerSessionWriter(pipeSess3, defaultSessionWriteTimeout)

	gcDone3 := make(chan struct{})
	go func() {
		defer close(gcDone3)
		time.Sleep(15 * time.Millisecond)
		_ = clientConn3.SetWriteDeadline(time.Unix(1, 0))
		GetGettyClientHandlerInstance().OnClose(pipeSess3)
		pipeSess3.conn = nil
	}()
	GetGettyClientHandlerInstance().OnCron(pipeSess3)
	<-gcDone3
	assert.Equal(t, "session-closed", safeSessionLabel(pipeSess3))
	_, cachedAfterClose := sessionWriters.Load(pipeSess3)
	assert.False(t, cachedAfterClose, "safeSessionLabel and post-close OnCron must not re-cache closed session in sessionWriters")
	GetGettyClientHandlerInstance().OnCron(pipeSess3)
	_, cachedAfterLateCron := sessionWriters.Load(pipeSess3)
	assert.False(t, cachedAfterLateCron, "late OnCron after OnClose must not insert closed session into sessionWriters")
}
