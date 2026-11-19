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
	"encoding/binary"
	"flag"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	getty "github.com/apache/dubbo-getty"
	"github.com/stretchr/testify/require"

	"seata.apache.org/seata-go/v2/pkg/discovery"
	"seata.apache.org/seata-go/v2/pkg/protocol/codec"
	"seata.apache.org/seata-go/v2/pkg/protocol/message"
	"seata.apache.org/seata-go/v2/pkg/remoting/config"
	"seata.apache.org/seata-go/v2/pkg/util/log"
)

// TestSessionManagerRedialsServerThatClosedConnection runs real getty clients
// against a minimal TC. getty does not redial a connection that the server
// closed, which is what a TC restart looks like, so the TC only sees a second
// connection if the manager restarts the client.
func TestSessionManagerRedialsServerThatClosedConnection(t *testing.T) {
	initReconnectTest.Do(func() {
		codec.Init()
		log.Init()
		GetGettyClientHandlerInstance().RegisterProcessor(message.MessageTypeRegCltResult, notifyResponseProcessor{})
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	conns := make(chan net.Conn, 4)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveRegisterTM(conn)
			conns <- conn
		}
	}()

	gettyConf := &config.Config{}
	flags := flag.NewFlagSet("getty", flag.ContinueOnError)
	gettyConf.RegisterFlagsWithPrefix("getty", flags)
	require.NoError(t, flags.Parse([]string{
		"-getty.session.tcp-read-timeout=100ms",
		"-getty.session.cron-period=100ms",
	}))
	manager := &SessionManager{
		gettyConf:   gettyConf,
		seataConfig: &config.SeataConfig{ApplicationID: "reconnect-test", TxServiceGroup: "default_tx_group"},
	}
	// The getty event listener reports to the package-level manager.
	previous := sessionManager
	sessionManager = manager
	var accepted []net.Conn
	t.Cleanup(func() {
		// getty v1.5.0 races with itself when a client is closed while one of
		// its sessions is still reading, so end the sessions from the server
		// side first and close the clients once they are gone.
		listener.Close()
		for _, conn := range accepted {
			conn.Close()
		}
		deadline := time.Now().Add(2 * time.Second)
		for atomic.LoadInt32(&manager.sessionSize) > 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		manager.refreshServerList(nil)
		// Closed clients leave their dial loop on its next wake-up.
		time.Sleep(500 * time.Millisecond)
		sessionManager = previous
	})

	port := listener.Addr().(*net.TCPAddr).Port
	manager.refreshServerList([]*discovery.ServiceInstance{{Addr: "127.0.0.1", Port: port}})

	accepted = append(accepted, nextAcceptedConn(t, conns))
	firstSession := waitNewReadySession(t, manager, nil)
	// getty's first event loop dials again if the session drops within one
	// reconnect interval, so wait until only the manager can redial.
	time.Sleep(time.Second)
	// A restarting TC closes its connections and the client reads EOF.
	require.NoError(t, accepted[0].(*net.TCPConn).CloseWrite())
	accepted = append(accepted, nextAcceptedConn(t, conns))
	waitNewReadySession(t, manager, firstSession)
	require.Equal(t, int32(1), atomic.LoadInt32(&manager.sessionSize))
}

var initReconnectTest sync.Once

// notifyResponseProcessor completes the future of a response, like the client
// response processor that a test in this package cannot import.
type notifyResponseProcessor struct{}

func (notifyResponseProcessor) Process(_ context.Context, rpcMessage message.RpcMessage) error {
	GetGettyRemotingClient().NotifyRpcMessageResponse(rpcMessage)
	return nil
}

// serveRegisterTM accepts every TM registration on conn and ignores anything
// else, until conn is closed.
func serveRegisterTM(conn net.Conn) {
	var pending []byte
	chunk := make([]byte, 4096)
	for {
		n, err := conn.Read(chunk)
		if err != nil {
			return
		}
		pending = append(pending, chunk[:n]...)
		// The total frame length follows the two magic bytes and the version.
		for len(pending) >= 7 {
			total := int(binary.BigEndian.Uint32(pending[3:7]))
			if len(pending) < total {
				break
			}
			pkg, _, err := rpcPkgHandler.Read(nil, pending[:total])
			pending = pending[total:]
			if err != nil {
				return
			}
			request := pkg.(message.RpcMessage)
			if _, ok := request.Body.(message.RegisterTMRequest); !ok {
				continue
			}
			response, err := rpcPkgHandler.Write(nil, message.RpcMessage{
				ID:    request.ID,
				Type:  message.RequestTypeResponse,
				Codec: request.Codec,
				Body: message.RegisterTMResponse{AbstractIdentifyResponse: message.AbstractIdentifyResponse{
					Identified: true,
				}},
			})
			if err != nil {
				return
			}
			if _, err := conn.Write(response); err != nil {
				return
			}
		}
	}
}

func nextAcceptedConn(t *testing.T, conns <-chan net.Conn) net.Conn {
	t.Helper()

	select {
	case conn := <-conns:
		return conn
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the client to connect")
	}
	return nil
}

// waitNewReadySession waits until the manager has a ready session other than
// previous and returns it.
func waitNewReadySession(t *testing.T, manager *SessionManager, previous getty.Session) getty.Session {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, session := range manager.readySessions() {
			if session != previous {
				return session
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for a ready session")
	return nil
}
