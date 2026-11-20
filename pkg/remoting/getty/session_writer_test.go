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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	getty "github.com/apache/dubbo-getty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"seata.apache.org/seata-go/v2/pkg/protocol/message"
)

type gcAwareTestSession struct {
	getty.Session
	closed       atomic.Bool
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
		connTimeout: defaultSessionWriteTimeout,
		onWrite:     onWrite,
	}
}

func (s *gcAwareTestSession) IsClosed() bool {
	return s.closed.Load()
}

func (s *gcAwareTestSession) Close() {
	s.closed.Store(true)
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
