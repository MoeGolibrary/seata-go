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
	"testing"

	"github.com/stretchr/testify/require"

	"seata.apache.org/seata-go/v2/pkg/protocol/message"
	"seata.apache.org/seata-go/v2/pkg/remoting/processor"
)

func TestOnMessageWhileRegisteringProcessor(t *testing.T) {
	handler := &gettyClientHandler{processorMap: make(map[message.MessageType]processor.RemotingProcessor)}
	processed := &countingProcessor{}
	response := message.RegisterTMResponse{}
	handler.RegisterProcessor(response.GetTypeCode(), processed)

	const messages = 100
	start := make(chan struct{})
	registered := make(chan struct{})
	go func() {
		<-start
		for i := 0; i < messages; i++ {
			handler.RegisterProcessor(response.GetTypeCode(), processed)
		}
		close(registered)
	}()

	close(start)
	for i := 0; i < messages; i++ {
		handler.OnMessage(nil, message.RpcMessage{Body: response})
	}
	<-registered
	require.Equal(t, messages, processed.count)
}

type countingProcessor struct{ count int }

func (p *countingProcessor) Process(context.Context, message.RpcMessage) error {
	p.count++
	return nil
}
