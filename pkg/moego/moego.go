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

// Package moego 是 MoeGolibrary/seata-go fork 的标记包，上游没有这个包。
//
// MoeGo 的 seata 封装会 import 本包。某个服务的 go.mod 如果漏了
//
//	replace seata.apache.org/seata-go/v2 => github.com/MoeGolibrary/seata-go/v2 <版本>
//
// 编译时就会报找不到本包，避免悄悄用上缺少补丁的官方版本。
// 补丁清单、发版和同步上游的流程见仓库根目录的 MOEGO_PATCHES.md。
package moego

const (
	// UpstreamBase 是本 fork 所基于的上游 master 提交。
	UpstreamBase = "0dde9d23592d"

	// Version 是 fork 的版本号，发版时改成与 git tag 一致；未发版时为 "dev"。
	Version = "dev"
)
