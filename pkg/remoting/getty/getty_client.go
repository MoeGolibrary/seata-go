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
	"sync"

	getty "github.com/apache/dubbo-getty"
	gxtime "github.com/dubbogo/gost/time"
	"go.uber.org/atomic"

	"seata.apache.org/seata-go/v2/pkg/protocol/codec"
	"seata.apache.org/seata-go/v2/pkg/protocol/message"
	"seata.apache.org/seata-go/v2/pkg/util/log"
)

var (
	gettyRemotingClient     *GettyRemotingClient
	onceGettyRemotingClient = &sync.Once{}
)

type GettyRemotingClient struct {
	idGenerator   *atomic.Uint32
	gettyRemoting *GettyRemoting
}

func GetGettyRemotingClient() *GettyRemotingClient {
	if gettyRemotingClient == nil {
		onceGettyRemotingClient.Do(func() {
			gettyRemotingClient = &GettyRemotingClient{
				idGenerator:   &atomic.Uint32{},
				gettyRemoting: newGettyRemoting(),
			}
		})
	}
	return gettyRemotingClient
}

func (client *GettyRemotingClient) SendAsyncRequest(msg interface{}) error {
	rpcMessage := newAsyncRequestMessage(int32(client.idGenerator.Inc()), msg)
	return client.gettyRemoting.SendAsync(rpcMessage, nil, client.asyncCallback)
}

func newAsyncRequestMessage(id int32, msg interface{}) message.RpcMessage {
	var msgType message.RequestType
	if _, ok := msg.(message.HeartBeatMessage); ok {
		msgType = message.RequestTypeHeartbeatRequest
	} else {
		msgType = message.RequestTypeRequestOneway
	}
	return message.RpcMessage{
		ID:         id,
		Type:       msgType,
		Codec:      byte(codec.CodecTypeSeata),
		Compressor: 0,
		Body:       msg,
	}
}

func (client *GettyRemotingClient) SendAsyncResponse(msgID int32, msg interface{}) error {
	rpcMessage := message.RpcMessage{
		ID:         msgID,
		Type:       message.RequestTypeResponse,
		Codec:      byte(codec.CodecTypeSeata),
		Compressor: 0,
		Body:       msg,
	}
	return client.gettyRemoting.SendAsync(rpcMessage, nil, nil)
}

type sessionContextKey struct{}

// WithSession binds a session to ctx. Requests and responses sent with the
// returned context go to that session instead of a load-balanced one.
func WithSession(ctx context.Context, session getty.Session) context.Context {
	return context.WithValue(ctx, sessionContextKey{}, session)
}

// SessionFromContext returns the session bound to ctx, or nil.
func SessionFromContext(ctx context.Context) getty.Session {
	if ctx == nil {
		return nil
	}
	session, _ := ctx.Value(sessionContextKey{}).(getty.Session)
	return session
}

// SendSyncRequestInContext sends a synchronous request on the session bound to
// ctx. Without a bound session it behaves like SendSyncRequest.
func (client *GettyRemotingClient) SendSyncRequestInContext(ctx context.Context, msg interface{}) (interface{}, error) {
	rpcMessage := message.RpcMessage{
		ID:         int32(client.idGenerator.Inc()),
		Type:       message.RequestTypeRequestSync,
		Codec:      byte(codec.CodecTypeSeata),
		Compressor: 0,
		Body:       msg,
	}
	return client.gettyRemoting.SendSync(rpcMessage, SessionFromContext(ctx), client.syncCallback)
}

// SendAsyncResponseInContext replies on the session bound to ctx, which must be
// the session the request arrived on: the TC that sent the request is the only
// one waiting for the response, and message ids are not unique across TC
// nodes. Without a bound session it behaves like SendAsyncResponse.
func (client *GettyRemotingClient) SendAsyncResponseInContext(ctx context.Context, msgID int32, msg interface{}) error {
	rpcMessage := message.RpcMessage{
		ID:         msgID,
		Type:       message.RequestTypeResponse,
		Codec:      byte(codec.CodecTypeSeata),
		Compressor: 0,
		Body:       msg,
	}
	return client.gettyRemoting.SendAsync(rpcMessage, SessionFromContext(ctx), nil)
}

// ForEachReadySession calls fn once for every ready session, with that session
// bound to the context. It waits like a normal request when no session is ready
// yet. A session whose call fails is released so that it reconnects and runs
// its registration again; an error is returned only if no call succeeded.
func (client *GettyRemotingClient) ForEachReadySession(fn func(ctx context.Context) error) error {
	if sessionManager == nil {
		return fmt.Errorf("session is closed")
	}
	sessions := sessionManager.waitReadySessions()
	if len(sessions) == 0 {
		return fmt.Errorf("no ready session")
	}
	var errs []error
	succeeded := 0
	for _, session := range sessions {
		if err := fn(WithSession(context.Background(), session)); err != nil {
			log.Errorf("request on session %s failed, release it: %v", session.Stat(), err)
			sessionManager.releaseSession(session)
			errs = append(errs, err)
			continue
		}
		succeeded++
	}
	if succeeded == 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (client *GettyRemotingClient) SendSyncRequest(msg interface{}) (interface{}, error) {
	rpcMessage := message.RpcMessage{
		ID:         int32(client.idGenerator.Inc()),
		Type:       message.RequestTypeRequestSync,
		Codec:      byte(codec.CodecTypeSeata),
		Compressor: 0,
		Body:       msg,
	}
	return client.gettyRemoting.SendSync(rpcMessage, nil, client.syncCallback)
}

func (g *GettyRemotingClient) asyncCallback(reqMsg message.RpcMessage, respMsg *message.MessageFuture) (interface{}, error) {
	go g.syncCallback(reqMsg, respMsg)
	return nil, nil
}

func (g *GettyRemotingClient) syncCallback(reqMsg message.RpcMessage, respMsg *message.MessageFuture) (interface{}, error) {
	// The future lives in futures, not mergeMsgMap, and nothing else removes
	// it. Once this returns nobody waits for the response any more, on either
	// branch, so this is the one place that owns the cleanup.
	defer g.gettyRemoting.RemoveMessageFuture(reqMsg.ID)
	select {
	case <-gxtime.GetDefaultTimerWheel().After(rpcRequestTimeout):
		log.Errorf("wait resp timeout: %#v", reqMsg)
		return nil, fmt.Errorf("wait response timeout, request: %#v", reqMsg)
	case <-respMsg.Done:
		return respMsg.Response, respMsg.Err
	}
}

func (client *GettyRemotingClient) GetMergedMessage(msgID int32) *message.MergedWarpMessage {
	return client.gettyRemoting.GetMergedMessage(msgID)
}

func (client *GettyRemotingClient) GetMessageFuture(msgID int32) *message.MessageFuture {
	return client.gettyRemoting.GetMessageFuture(msgID)
}

func (client *GettyRemotingClient) RemoveMessageFuture(msgID int32) {
	client.gettyRemoting.RemoveMessageFuture(msgID)
}

func (client *GettyRemotingClient) RemoveMergedMessageFuture(msgID int32) {
	client.gettyRemoting.RemoveMergedMessageFuture(msgID)
}

func (client *GettyRemotingClient) NotifyRpcMessageResponse(msg message.RpcMessage) {
	client.gettyRemoting.NotifyRpcMessageResponse(msg)
}
