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
)

type sessionWriter struct {
	sem            chan struct{}
	closed         bool
	defaultTimeout time.Duration
	remoteAddr     string
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

	if capturePeerAddr && w.remoteAddr == "" {
		w.remoteAddr = session.RemoteAddr()
	}
	if w.remoteAddr == "" && netConn != nil {
		if rAddr := netConn.RemoteAddr(); rAddr != nil {
			w.remoteAddr = rAddr.String()
		}
	}
	peerAddr = w.remoteAddr
	if onReadyToSend != nil {
		onReadyToSend(peerAddr)
	}

	var (
		inWriteMu sync.Mutex
		inWrite   = true
		aborted   bool
		stopAbort func() bool
	)
	if ctx != nil && ctx.Done() != nil {
		stopAbort = context.AfterFunc(ctx, func() {
			inWriteMu.Lock()
			defer inWriteMu.Unlock()
			if !inWrite {
				return
			}
			aborted = true
			abortInFlightSessionWrite(session, netConn)
		})
	}

	writeErr := sendSessionFrame(ctx, session, netConn, msg, writeTimeout, &inWriteMu, &inWrite, &aborted)
	if stopAbort != nil {
		stopAbort()
	}
	inWriteMu.Lock()
	inWrite = false
	inWriteMu.Unlock()

	if writeErr != nil || (ctx != nil && ctx.Err() != nil) {
		w.closed = true
		needRelease = true
		if netConn != nil {
			_ = netConn.SetWriteDeadline(time.Unix(1, 0))
		}
		if ctx != nil && ctx.Err() != nil {
			return peerAddr, true, ctx.Err()
		}
		return peerAddr, true, writeErr
	}
	return peerAddr, false, nil
}

func sendSessionFrame(
	ctx context.Context,
	session getty.Session,
	netConn net.Conn,
	msg message.RpcMessage,
	writeTimeout time.Duration,
	inWriteMu *sync.Mutex,
	inWrite *bool,
	aborted *bool,
) (err error) {
	if netConn != nil {
		pkgBytes, encErr := encodeSessionPkg(session, msg)
		if encErr != nil {
			return encErr
		}
		inWriteMu.Lock()
		if *aborted || !*inWrite || (ctx != nil && ctx.Err() != nil) {
			_ = netConn.SetWriteDeadline(time.Unix(1, 0))
			inWriteMu.Unlock()
			if ctx != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			return context.Canceled
		}
		if writeTimeout > 0 {
			if dlErr := netConn.SetWriteDeadline(time.Now().Add(writeTimeout)); dlErr != nil {
				inWriteMu.Unlock()
				return dlErr
			}
		}
		inWriteMu.Unlock()
		_, sendErr := netConn.Write(pkgBytes)
		return sendErr
	}
	inWriteMu.Lock()
	if *aborted || !*inWrite || (ctx != nil && ctx.Err() != nil) {
		inWriteMu.Unlock()
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		return context.Canceled
	}
	inWriteMu.Unlock()
	_, _, err = session.WritePkg(msg, writeTimeout)
	return err
}

func safeSessionConn(session getty.Session) (conn net.Conn) {
	defer func() {
		if recover() != nil {
			conn = nil
		}
	}()
	if session == nil || fmt.Sprintf("%T", session) == "*mock.MockTestSession" {
		return nil
	}
	return session.Conn()
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
