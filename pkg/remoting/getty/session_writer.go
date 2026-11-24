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
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	getty "github.com/apache/dubbo-getty"

	"seata.apache.org/seata-go/v2/pkg/protocol/message"
)

const defaultSessionWriteTimeout = 5 * time.Second

var (
	errSessionClosedSentinel = errors.New("session is closed")
	sessionWriters           sync.Map // map[getty.Session]*sessionWriter
	encodeSessionPkg         = func(session getty.Session, msg message.RpcMessage) ([]byte, error) {
		return rpcPkgHandler.Write(session, msg)
	}
	sessionConnOf = func(session getty.Session) net.Conn {
		return session.Conn()
	}
)

type sessionWriter struct {
	sem            chan struct{}
	closed         bool
	defaultTimeout time.Duration
	addrMu         sync.RWMutex
	remoteAddr     string
}

func (w *sessionWriter) getRemoteAddr() string {
	w.addrMu.RLock()
	defer w.addrMu.RUnlock()
	return w.remoteAddr
}

func (w *sessionWriter) setRemoteAddr(addr string) {
	if addr == "" {
		return
	}
	w.addrMu.Lock()
	if w.remoteAddr == "" {
		w.remoteAddr = addr
	}
	w.addrMu.Unlock()
}

func newSessionWriter(defaultTimeout time.Duration, remoteAddr string) *sessionWriter {
	if defaultTimeout <= 0 {
		defaultTimeout = defaultSessionWriteTimeout
	}
	return &sessionWriter{
		sem:            make(chan struct{}, 1),
		defaultTimeout: defaultTimeout,
		remoteAddr:     remoteAddr,
	}
}

func registerSessionWriter(session getty.Session, defaultTimeout time.Duration) {
	if session == nil {
		return
	}
	sessionWriters.Store(session, newSessionWriter(defaultTimeout, session.RemoteAddr()))
}

func getOrCreateSessionWriter(session getty.Session) *sessionWriter {
	if existing, ok := sessionWriters.Load(session); ok {
		return existing.(*sessionWriter)
	}
	created := newSessionWriter(defaultSessionWriteTimeout, "")
	actual, _ := sessionWriters.LoadOrStore(session, created)
	return actual.(*sessionWriter)
}

func safeSessionLabel(session getty.Session) string {
	if session == nil {
		return "session-closed"
	}
	if existing, ok := sessionWriters.Load(session); ok {
		w := existing.(*sessionWriter)
		if addr := w.getRemoteAddr(); addr != "" {
			return addr
		}
		w.sem <- struct{}{}
		if !w.closed && !session.IsClosed() {
			addr := session.RemoteAddr()
			w.setRemoteAddr(addr)
			<-w.sem
			if addr != "" {
				return addr
			}
			return "session"
		}
		addr := w.getRemoteAddr()
		<-w.sem
		if addr != "" {
			return addr
		}
	}
	if sessionManager != nil {
		if addr, ok := sessionManager.sessionAddresses.Load(session); ok {
			if s, isStr := addr.(string); isStr && s != "" {
				return s
			}
		}
	}
	if session.IsClosed() {
		return "session-closed"
	}
	return "session"
}

// closeSessionWriter serializes session teardown with in-flight WritePkg calls
// inside OnClose, right before dubbo-getty's session.gc() sets s.Connection = nil.
func closeSessionWriter(session getty.Session) {
	if session == nil {
		return
	}
	w := getOrCreateSessionWriter(session)
	w.sem <- struct{}{}
	w.closed = true
	<-w.sem
	sessionWriters.CompareAndDelete(session, w)
}

func writeSessionPkg(
	ctx context.Context,
	session getty.Session,
	msg message.RpcMessage,
	capturePeerAddr bool,
	onReadyToSend func(peerAddr string),
	closeOnWriteErr bool,
) (peerAddr string, writeFailed bool, err error) {
	if session == nil {
		return "", false, fmt.Errorf("session is closed")
	}
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", false, ctxErr
		}
	}

	w := getOrCreateSessionWriter(session)
	if ctx != nil && ctx.Done() != nil {
		select {
		case <-ctx.Done():
			return "", false, ctx.Err()
		case w.sem <- struct{}{}:
		}
	} else {
		w.sem <- struct{}{}
	}

	var (
		needRelease bool
		netConn     net.Conn
	)
	defer func() {
		<-w.sem
		if needRelease {
			releaseBrokenSession(session, netConn, w, closeOnWriteErr)
		}
	}()

	if w.closed || session.IsClosed() {
		if session.IsClosed() {
			sessionWriters.CompareAndDelete(session, w)
		}
		return "", false, errSessionClosedSentinel
	}

	netConn = safeSessionConn(session)

	writeTimeout := w.defaultTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultSessionWriteTimeout
	}
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", false, ctxErr
		}
		if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
			rem := time.Until(deadline)
			if rem <= 0 {
				return "", false, context.DeadlineExceeded
			}
			if rem < writeTimeout {
				writeTimeout = rem
			}
		}
	}

	if capturePeerAddr && w.getRemoteAddr() == "" {
		w.setRemoteAddr(session.RemoteAddr())
	}
	if w.getRemoteAddr() == "" && netConn != nil {
		if rAddr := netConn.RemoteAddr(); rAddr != nil {
			w.setRemoteAddr(rAddr.String())
		}
	}
	peerAddr = w.getRemoteAddr()
	if onReadyToSend != nil {
		onReadyToSend(peerAddr)
	}

	started, writeErr := sendSessionFrame(ctx, session, netConn, msg, writeTimeout)
	if writeErr != nil && started {
		w.closed = true
		needRelease = true
		if netConn != nil {
			_ = netConn.SetWriteDeadline(time.Unix(1, 0))
		}
		return peerAddr, true, writeErr
	}
	if writeErr != nil {
		return peerAddr, false, writeErr
	}
	return peerAddr, false, nil
}

func isCompressEncodingEnabled() bool {
	return sessionManager != nil && sessionManager.gettyConf.SessionConfig.CompressEncoding
}

func sendSessionFrame(
	ctx context.Context,
	session getty.Session,
	netConn net.Conn,
	msg message.RpcMessage,
	writeTimeout time.Duration,
) (started bool, err error) {
	// When Getty zip compression is enabled on the session, fall back to session.WritePkg
	// so frames continue flowing through dubbo-getty's compressed connection wrapper.
	if netConn != nil && !isCompressEncodingEnabled() {
		pkgBytes, encErr := encodeSessionPkg(session, msg)
		if encErr != nil {
			return false, encErr
		}
		var (
			inWriteMu sync.Mutex
			inWrite   bool
			stopAbort func() bool
		)
		inWriteMu.Lock()
		if ctx != nil && ctx.Err() != nil {
			inWriteMu.Unlock()
			return false, ctx.Err()
		}
		if writeTimeout > 0 {
			if dlErr := netConn.SetWriteDeadline(time.Now().Add(writeTimeout)); dlErr != nil {
				inWriteMu.Unlock()
				return false, dlErr
			}
		}
		inWrite = true
		if ctx != nil && ctx.Done() != nil {
			stopAbort = context.AfterFunc(ctx, func() {
				inWriteMu.Lock()
				defer inWriteMu.Unlock()
				if !inWrite {
					return
				}
				abortInFlightSessionWrite(session, netConn)
			})
		}
		inWriteMu.Unlock()

		_, sendErr := netConn.Write(pkgBytes)
		abortedCallbackFired := stopAbort != nil && !stopAbort()
		inWriteMu.Lock()
		inWrite = false
		inWriteMu.Unlock()

		if sendErr != nil {
			if ctx != nil && ctx.Err() != nil {
				return true, ctx.Err()
			}
			return true, sendErr
		}
		if abortedCallbackFired {
			// The full frame was already written before or as ctx expired; restore a clean
			// deadline so subsequent writes on this shared session are not affected.
			_ = netConn.SetWriteDeadline(time.Time{})
		}
		return true, nil
	}

	var (
		inWriteMu sync.Mutex
		inWrite   bool
		stopAbort func() bool
	)
	inWriteMu.Lock()
	if ctx != nil && ctx.Err() != nil {
		inWriteMu.Unlock()
		return false, ctx.Err()
	}
	inWrite = true
	if ctx != nil && ctx.Done() != nil {
		stopAbort = context.AfterFunc(ctx, func() {
			inWriteMu.Lock()
			defer inWriteMu.Unlock()
			if !inWrite {
				return
			}
			abortInFlightSessionWrite(session, nil)
		})
	}
	inWriteMu.Unlock()

	_, _, err = session.WritePkg(msg, writeTimeout)
	abortedCallbackFired := stopAbort != nil && !stopAbort()
	inWriteMu.Lock()
	inWrite = false
	inWriteMu.Unlock()

	if err != nil {
		if ctx != nil && ctx.Err() != nil {
			return true, ctx.Err()
		}
		return true, err
	}
	if abortedCallbackFired && session.IsClosed() {
		if ctx != nil && ctx.Err() != nil {
			return true, ctx.Err()
		}
		return true, errSessionClosedSentinel
	}
	return true, nil
}

func safeSessionConn(session getty.Session) (conn net.Conn) {
	defer func() {
		if recover() != nil {
			conn = nil
		}
	}()
	if session == nil || sessionConnOf == nil {
		return nil
	}
	return sessionConnOf(session)
}

func abortInFlightSessionWrite(session getty.Session, netConn net.Conn) {
	if netConn != nil {
		// Directly expire the write deadline on the underlying net.Conn so an in-flight
		// netConn.Write unblocks immediately without invoking dubbo-getty's session.stop()
		// (which would reset SetWriteDeadline(now + s.WriteTimeout()) and race on gettyConn).
		_ = netConn.SetWriteDeadline(time.Unix(1, 0))
		return
	}
	defer func() { _ = recover() }()
	if !session.IsClosed() {
		session.Close()
	}
}

func releaseBrokenSession(session getty.Session, netConn net.Conn, w *sessionWriter, closeOnWriteErr bool) {
	defer func() {
		if netConn != nil {
			_ = netConn.SetWriteDeadline(time.Unix(1, 0))
		}
	}()
	if sessionManager != nil {
		if _, registered := sessionManager.allSessions.Load(session); registered {
			sessionManager.releaseSession(session)
			sessionWriters.CompareAndDelete(session, w)
			return
		}
	}
	if closeOnWriteErr && !session.IsClosed() {
		session.Close()
		sessionWriters.CompareAndDelete(session, w)
	}
}

func summarizeRpcMessage(msg message.RpcMessage) string {
	switch body := msg.Body.(type) {
	case message.BranchRegisterRequest:
		return fmt.Sprintf(
			"RpcMessage{ID:%d, Type:%d, Body:BranchRegisterRequest{Xid:%q, ResourceId:%q, BranchType:%d}}",
			msg.ID, msg.Type, body.Xid, body.ResourceId, body.BranchType,
		)
	case *message.BranchRegisterRequest:
		if body != nil {
			return fmt.Sprintf(
				"RpcMessage{ID:%d, Type:%d, Body:BranchRegisterRequest{Xid:%q, ResourceId:%q, BranchType:%d}}",
				msg.ID, msg.Type, body.Xid, body.ResourceId, body.BranchType,
			)
		}
	case message.BranchReportRequest:
		return fmt.Sprintf(
			"RpcMessage{ID:%d, Type:%d, Body:BranchReportRequest{Xid:%q, BranchId:%d, ResourceId:%q, Status:%d}}",
			msg.ID, msg.Type, body.Xid, body.BranchId, body.ResourceId, body.Status,
		)
	case *message.BranchReportRequest:
		if body != nil {
			return fmt.Sprintf(
				"RpcMessage{ID:%d, Type:%d, Body:BranchReportRequest{Xid:%q, BranchId:%d, ResourceId:%q, Status:%d}}",
				msg.ID, msg.Type, body.Xid, body.BranchId, body.ResourceId, body.Status,
			)
		}
	}
	return fmt.Sprintf("RpcMessage{ID:%d, Type:%d, BodyType:%T}", msg.ID, msg.Type, msg.Body)
}
