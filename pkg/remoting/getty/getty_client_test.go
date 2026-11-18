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
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	getty "github.com/apache/dubbo-getty"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"seata.apache.org/seata-go/v2/pkg/discovery"
	"seata.apache.org/seata-go/v2/pkg/protocol/codec"
	"seata.apache.org/seata-go/v2/pkg/protocol/message"
	"seata.apache.org/seata-go/v2/pkg/remoting/mock"
	"seata.apache.org/seata-go/v2/pkg/util/log"
)

// TestGettyRemotingClient_SendSyncRequest unit test for SendSyncRequest function
func TestGettyRemotingClient_SendSyncRequest(t *testing.T) {
	respMsg := message.GlobalBeginResponse{
		AbstractTransactionResponse: message.AbstractTransactionResponse{
			AbstractResultMessage: message.AbstractResultMessage{
				ResultCode: message.ResultCodeSuccess,
			},
		},
	}
	patches := gomonkey.ApplyMethod(reflect.TypeOf(GetGettyRemotingClient().gettyRemoting), "SendSync",
		func(_ *GettyRemoting, msg message.RpcMessage, s getty.Session, callback callbackMethod) (interface{},
			error) {
			return respMsg, nil
		})
	defer patches.Reset()
	resp, err := GetGettyRemotingClient().SendSyncRequest("message")
	assert.Empty(t, err)
	assert.Equal(t, respMsg, resp.(message.GlobalBeginResponse))
}

// TestGettyRemotingClient_SendAsyncResponse unit test for SendAsyncResponse function
func TestGettyRemotingClient_SendAsyncResponse(t *testing.T) {
	patches := gomonkey.ApplyMethod(reflect.TypeOf(GetGettyRemotingClient().gettyRemoting), "SendAsync",
		func(_ *GettyRemoting, msg message.RpcMessage, s getty.Session, callback callbackMethod) error {
			return nil
		})
	defer patches.Reset()
	err := GetGettyRemotingClient().SendAsyncResponse(1, "message")
	assert.Empty(t, err)
}

// TestGettyRemotingClient_SendInContextUsesBoundSession checks that requests and
// responses bound to a session through the context are written to that session
// instead of a load-balanced one.
func TestGettyRemotingClient_SendInContextUsesBoundSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := mock.NewMockTestSession(ctrl)
	ctx := WithSession(context.Background(), session)

	var syncSession, asyncSession getty.Session
	patches := gomonkey.ApplyMethod(reflect.TypeOf(GetGettyRemotingClient().gettyRemoting), "SendSync",
		func(_ *GettyRemoting, msg message.RpcMessage, s getty.Session, callback callbackMethod) (interface{}, error) {
			syncSession = s
			return nil, nil
		})
	patches.ApplyMethod(reflect.TypeOf(GetGettyRemotingClient().gettyRemoting), "SendAsync",
		func(_ *GettyRemoting, msg message.RpcMessage, s getty.Session, callback callbackMethod) error {
			asyncSession = s
			return nil
		})
	defer patches.Reset()

	_, err := GetGettyRemotingClient().SendSyncRequestInContext(ctx, "request")
	assert.NoError(t, err)
	assert.Equal(t, getty.Session(session), syncSession)

	err = GetGettyRemotingClient().SendAsyncResponseInContext(ctx, 1, "response")
	assert.NoError(t, err)
	assert.Equal(t, getty.Session(session), asyncSession)

	// Without a bound session the caller keeps the old behaviour: a nil session
	// lets GettyRemoting pick one.
	err = GetGettyRemotingClient().SendAsyncResponseInContext(context.Background(), 2, "response")
	assert.NoError(t, err)
	assert.Nil(t, asyncSession)
}

// TestGettyRemotingClient_ForEachReadySession checks that a callback runs once for
// every ready session, skips sessions that are still registering, and releases a
// session whose callback fails so that it reconnects and registers again.
func TestGettyRemotingClient_ForEachReadySession(t *testing.T) {
	ctrl := gomock.NewController(t)
	manager := newTestSessionManager(nil)
	manager.refreshServerList([]*discovery.ServiceInstance{
		{Addr: "127.0.0.1", Port: 8091},
		{Addr: "127.0.0.2", Port: 8092},
		{Addr: "127.0.0.3", Port: 8093},
	})
	previous := sessionManager
	sessionManager = manager
	t.Cleanup(func() {
		sessionManager = previous
	})

	newSession := func(address string) *mock.MockTestSession {
		session := mock.NewMockTestSession(ctrl)
		session.EXPECT().IsClosed().Return(false).AnyTimes()
		session.EXPECT().RemoteAddr().Return(address).AnyTimes()
		session.EXPECT().Stat().Return(address).AnyTimes()
		return session
	}
	ok := newSession("127.0.0.1:8091")
	failing := newSession("127.0.0.2:8092")
	failing.EXPECT().Close().Times(1)
	pending := newSession("127.0.0.3:8093")
	manager.registerSession(ok)
	manager.registerSession(failing)
	manager.registerPendingSession(pending)

	var visited []getty.Session
	err := GetGettyRemotingClient().ForEachReadySession(func(ctx context.Context) error {
		session := SessionFromContext(ctx)
		visited = append(visited, session)
		if session == getty.Session(failing) {
			return errors.New("rejected")
		}
		return nil
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []getty.Session{ok, failing}, visited)
	assert.ElementsMatch(t, []getty.Session{ok}, manager.readySessions())

	// When every session fails, every session is released and the error is returned.
	ok.EXPECT().Close().Times(1)
	err = GetGettyRemotingClient().ForEachReadySession(func(context.Context) error {
		return errors.New("rejected")
	})
	assert.Error(t, err)
}

// TestGettyRemotingClient_SendAsyncRequest unit test for SendAsyncRequest function
func TestGettyRemotingClient_SendAsyncRequest(t *testing.T) {
	tests := []struct {
		name         string
		message      interface{}
		expectedType message.RequestType
	}{
		{
			name:         "HeartBeatMessage",
			message:      message.HeartBeatMessage{},
			expectedType: message.RequestTypeHeartbeatRequest,
		},
		{
			name:         "not HeartBeatMessage",
			message:      "message",
			expectedType: message.RequestTypeRequestOneway,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msg := newAsyncRequestMessage(1, test.message)
			assert.Equal(t, test.expectedType, msg.Type)
			assert.Equal(t, byte(codec.CodecTypeSeata), msg.Codec)
			assert.Equal(t, test.message, msg.Body)
		})
	}
}

// Test_syncCallback unit test for syncCallback function
func Test_syncCallback(t *testing.T) {
	codec.Init()
	log.Init()
	tests := []struct {
		name    string
		respMsg *message.MessageFuture
		reqMsg  message.RpcMessage
		wantErr bool
	}{
		{
			name: "timeout",
			respMsg: message.NewMessageFuture(message.RpcMessage{
				ID: 1,
			}),
			reqMsg: message.RpcMessage{
				ID: 2,
			},
			wantErr: true,
		},
		{
			name: "Done",
			respMsg: message.NewMessageFuture(message.RpcMessage{
				ID: 1,
			}),
			reqMsg: message.RpcMessage{
				ID: 2,
			},
			wantErr: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.wantErr {
				response, err := GetGettyRemotingClient().syncCallback(test.reqMsg, test.respMsg)
				assert.EqualError(t, err, fmt.Sprintf("wait response timeout, request: %#v", test.reqMsg))
				assert.Empty(t, response)
			} else {
				go func() {
					test.respMsg.Done <- struct{}{}
				}()
				response, err := GetGettyRemotingClient().syncCallback(test.reqMsg, test.respMsg)
				assert.Empty(t, err)
				assert.Empty(t, response)
			}
		})
	}
}

// Test_asyncCallback unit test for asyncCallback function
func Test_asyncCallback(t *testing.T) {
	tests := []struct {
		name    string
		respMsg *message.MessageFuture
		reqMsg  message.RpcMessage
		wantErr bool
	}{
		{
			name: "Done",
			respMsg: message.NewMessageFuture(message.RpcMessage{
				ID: 1,
			}),
			reqMsg: message.RpcMessage{
				ID: 2,
			},
			wantErr: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := GetGettyRemotingClient().asyncCallback(test.reqMsg, test.respMsg)
			assert.Empty(t, err)
			assert.Empty(t, response)
		})
	}
}
