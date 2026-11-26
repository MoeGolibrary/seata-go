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

package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"seata.apache.org/seata-go/v2/pkg/protocol/codec"
	"seata.apache.org/seata-go/v2/pkg/protocol/message"
	"seata.apache.org/seata-go/v2/pkg/remoting"
	"seata.apache.org/seata-go/v2/pkg/remoting/getty"
)

func TestInitPathHandlesImmediateTMResponse(t *testing.T) {
	const configEnv = "SEATA_TEST_STARTUP_CONFIG"
	if path := os.Getenv(configEnv); path != "" {
		ready := make(chan struct{}, 1)
		remoting.RegisterSessionOpenHook(func() error {
			select {
			case ready <- struct{}{}:
			default:
			}
			return nil
		})
		InitPath(path)
		select {
		case <-ready:
			require.Len(t, getty.GetGettyRemotingClient().ReadySessions(), 1)
		case <-time.After(5 * time.Second):
			t.Fatal("first TM handshake did not complete")
		}
		return
	}

	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer listener.Close()

	cfg := fmt.Sprintf(`seata:
  service:
    vgroup-mapping:
      default_tx_group: local
    grouplist:
      local: %q
`, listener.Addr().String())
	path := filepath.Join(t.TempDir(), "seatago.yml")
	require.NoError(t, os.WriteFile(path, []byte(cfg), 0o600))

	// Use a fresh process since InitPath installs SDK singletons and starts workers.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	codec.Init()
	replied := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			replied <- err
			return
		}
		defer conn.Close()
		replied <- replyToTMRegistration(conn)
		<-ctx.Done()
	}()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestInitPathHandlesImmediateTMResponse$")
	cmd.Env = append(os.Environ(), configEnv+"="+path)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.NoError(t, <-replied)
}

func replyToTMRegistration(conn net.Conn) error {
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	header := make([]byte, message.V1HeadLength)
	if _, err := io.ReadFull(conn, header); err != nil {
		return err
	}
	length := int(binary.BigEndian.Uint32(header[3:7]))
	if length < len(header) {
		return fmt.Errorf("invalid registration frame length %d", length)
	}
	frame := make([]byte, length)
	copy(frame, header)
	if _, err := io.ReadFull(conn, frame[len(header):]); err != nil {
		return err
	}

	handler := &getty.RpcPackageHandler{}
	pkg, _, err := handler.Read(nil, frame)
	if err != nil {
		return err
	}
	request := pkg.(message.RpcMessage)
	if _, ok := request.Body.(message.RegisterTMRequest); !ok {
		return fmt.Errorf("expected initial TM registration, got %T", request.Body)
	}
	reply, err := handler.Write(nil, message.RpcMessage{
		ID:    request.ID,
		Codec: request.Codec,
		Type:  message.RequestTypeResponse,
		Body: message.RegisterTMResponse{
			AbstractIdentifyResponse: message.AbstractIdentifyResponse{Identified: true},
		},
	})
	if err != nil {
		return err
	}
	_, err = conn.Write(reply)
	return err
}
