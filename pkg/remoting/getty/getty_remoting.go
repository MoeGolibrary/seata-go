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
	"time"

	getty "github.com/apache/dubbo-getty"

	"seata.apache.org/seata-go/v2/pkg/protocol/message"
	"seata.apache.org/seata-go/v2/pkg/remoting/rpc"
	"seata.apache.org/seata-go/v2/pkg/util/log"
)

const (
	RpcRequestTimeout = 20 * time.Second
)

// rpcRequestTimeout is the deadline the client actually waits on. It exists so
// tests can shorten the wait without changing the exported constant.
var rpcRequestTimeout = RpcRequestTimeout

type (
	callbackMethod func(reqMsg message.RpcMessage, respMsg *message.MessageFuture) (interface{}, error)
	GettyRemoting  struct {
		futures     *sync.Map
		mergeMsgMap *sync.Map
	}
)

func newGettyRemoting() *GettyRemoting {
	return &GettyRemoting{
		futures:     &sync.Map{},
		mergeMsgMap: &sync.Map{},
	}
}

func (g *GettyRemoting) SendSync(msg message.RpcMessage, s getty.Session, callback callbackMethod) (interface{}, error) {
	if s == nil {
		if sessionManager == nil {
			return nil, fmt.Errorf("session is closed")
		}
		s = sessionManager.selectSession(msg.Body)
	}
	if s == nil || s.IsClosed() {
		log.Warn("sendAsyncRequestWithResponse nothing, caused by null channel.")
		return nil, fmt.Errorf("session is closed")
	}
	result, addr, err := g.sendAsyncWithContext(context.Background(), s, msg, callback, true, false)
	if err != nil {
		log.Errorf("send message: %#v, peer: %s, err: %v", msg, addr, err)
		return nil, err
	}
	return result, nil
}

func (g *GettyRemoting) SendSyncWithContext(
	ctx context.Context,
	msg message.RpcMessage,
	s getty.Session,
	callback callbackMethod,
) (interface{}, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if s == nil {
		if sessionManager == nil {
			return nil, fmt.Errorf("session is closed")
		}
		var selErr error
		s, selErr = sessionManager.selectSessionWithContext(ctx, msg.Body)
		if selErr != nil {
			return nil, selErr
		}
	}
	if s == nil || s.IsClosed() {
		log.Warn("sendAsyncRequestWithResponse nothing, caused by null channel.")
		return nil, fmt.Errorf("session is closed")
	}
	result, addr, err := g.sendAsyncWithContext(ctx, s, msg, callback, true, true)
	if err != nil {
		log.Errorf("send message: %#v, peer: %s, err: %v", msg, addr, err)
		return nil, err
	}
	return result, nil
}

func (g *GettyRemoting) SendAsync(msg message.RpcMessage, s getty.Session, callback callbackMethod) error {
	if s == nil {
		if sessionManager == nil {
			return fmt.Errorf("session is closed")
		}
		s = sessionManager.selectSession(msg.Body)
	}
	if s == nil || s.IsClosed() {
		log.Warn("sendAsyncRequestWithResponse nothing, caused by null channel.")
		return fmt.Errorf("session is closed")
	}
	_, addr, err := g.sendAsyncWithContext(context.Background(), s, msg, callback, true, false)
	if err != nil {
		log.Errorf("send message: %#v, peer: %s, err: %v", msg, addr, err)
	}
	return err
}

func (g *GettyRemoting) sendAsync(session getty.Session, msg message.RpcMessage, callback callbackMethod) (interface{}, error) {
	res, _, err := g.sendAsyncWithContext(context.Background(), session, msg, callback, false, false)
	return res, err
}

func (g *GettyRemoting) sendAsyncWithContext(
	ctx context.Context,
	session getty.Session,
	msg message.RpcMessage,
	callback callbackMethod,
	trackRPCCount bool,
	closeOnWriteErr bool,
) (interface{}, string, error) {
	if _, ok := msg.Body.(message.HeartBeatMessage); ok {
		log.Debug("send async message: {%#v}", msg)
	} else {
		log.Infof("send async message: {%#v}", msg)
	}
	if session == nil {
		log.Warn("sendAsyncRequestWithResponse nothing, caused by null channel.")
		return nil, "", fmt.Errorf("session is closed")
	}
	// Only requests with a callback wait for a response, and we register the future
	// and read RemoteAddr only after acquiring the per-session write slot while !closed.
	var resp *message.MessageFuture
	counted := false
	var countedAddr string
	defer func() {
		if counted {
			rpc.EndCount(countedAddr)
		}
	}()

	peerAddr, writeFailed, err := writeSessionPkg(ctx, session, msg, trackRPCCount, func(addr string) {
		if trackRPCCount {
			counted = true
			countedAddr = addr
			rpc.BeginCount(addr)
		}
		if callback != nil {
			resp = message.NewMessageFuture(msg)
			g.futures.Store(msg.ID, resp)
		}
	}, closeOnWriteErr)
	if err != nil {
		if resp != nil {
			g.futures.Delete(msg.ID)
		}
		if errors.Is(err, errSessionClosedSentinel) {
			log.Warn("sendAsyncRequestWithResponse nothing, caused by null channel.")
			return nil, peerAddr, fmt.Errorf("session is closed")
		}
		if writeFailed && !closeOnWriteErr {
			log.Errorf("send message: %#v, session: %s", msg, session.Stat())
		}
		return nil, peerAddr, err
	}
	if callback != nil {
		res, cbErr := callback(msg, resp)
		return res, peerAddr, cbErr
	}
	return nil, peerAddr, nil
}

func (g *GettyRemoting) GetMessageFuture(msgID int32) *message.MessageFuture {
	if msg, ok := g.futures.Load(msgID); ok {
		return msg.(*message.MessageFuture)
	}
	return nil
}

func (g *GettyRemoting) RemoveMessageFuture(msgID int32) {
	g.futures.Delete(msgID)
}

func (g *GettyRemoting) RemoveMergedMessageFuture(msgID int32) {
	g.mergeMsgMap.Delete(msgID)
}

func (g *GettyRemoting) GetMergedMessage(msgID int32) *message.MergedWarpMessage {
	if msg, ok := g.mergeMsgMap.Load(msgID); ok {
		return msg.(*message.MergedWarpMessage)
	}
	return nil
}

func (g *GettyRemoting) NotifyRpcMessageResponse(rpcMessage message.RpcMessage) {
	messageFuture := g.GetMessageFuture(rpcMessage.ID)
	if messageFuture != nil {
		// todo add messageFuture.Err
		// messageFuture.Err = rpcMessage.Err
		if !messageFuture.Complete(rpcMessage.Body) {
			log.Warnf("response notification dropped for msg ID: %d because the future was already signaled", rpcMessage.ID)
		}
		// client.msgFutures.Delete(rpcMessage.RequestID)
	} else {
		log.Infof("msg: {} is not found in msgFutures.", rpcMessage.ID)
	}
}
