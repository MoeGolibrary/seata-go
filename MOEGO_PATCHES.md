<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# MoeGo fork 说明

本仓库是 [apache/incubator-seata-go](https://github.com/apache/incubator-seata-go) 的 fork，由 MoeGo Fintech 维护。
只放通用的缺陷修复和一个标记包，不放任何 MoeGo 业务代码或配置；MoeGo 特有的封装（拦截器、PostgreSQL fence、配置渲染等）在 go-lib 里。

设计文档：https://mengshikeji.feishu.cn/wiki/CBuew0xsUiTvxzkCIyWc3voynhg（"五：Version Selection"）

## 基线

- 分支 `moego/main`，基于上游 master `0dde9d23592d`（2026-10-27）。
- 选 master 而不是 v2.1.0 或 v2.2.0-rc01 的原因：#1202、#1195、#1164、#1163 四个二阶段相关修复只在 master 上。
- `master` 分支保持与上游一致，不做改动。

## 引用方式

模块路径不改，仍是 `seata.apache.org/seata-go/v2`。用到的服务在自己的 go.mod 里加：

```
replace seata.apache.org/seata-go/v2 => github.com/MoeGolibrary/seata-go/v2 <版本>
```

- `replace` 只在服务自己的 go.mod 里生效，库（如 go-lib）里写了不会传给下游。
- 漏加时会悄悄用上官方版本，所以 MoeGo 的封装会 import 本仓库独有的 `pkg/moego` 包：漏加 `replace` 就会编译失败。
- 上游正式版包含了下表的补丁后，删掉 `replace` 即可切回官方版本，业务代码不用改。

## 补丁清单

| # | 内容 | 状态 | 上游 issue / PR | 提交 |
|-|-|-|-|-|
| 0 | 标记包 `pkg/moego` 与本文件（只留在 fork，不提上游） | 已完成 | — | — |
| 1 | TC 地址写 DNS 名时，连接建立后立即被关闭（"skip session for removed server address"）；改为按解析后的地址匹配 | 待实现 | 待提交 | — |
| 2 | RM 只通过一条会话向 TC 注册资源，TC 多副本时其他节点下发二阶段报 "rm client is not connected"；改为在每条会话建立（含重连）时都注册 | 待实现 | 待提交 | — |

## 发版流程

1. 每个补丁一个提交，提交说明里写清问题、复现方式和上游 issue / PR 链接。
2. 跑上游单元测试：`go test ./...`。
3. 用本地多 TC 环境跑完设计文档第十二章的验收用例。
4. 把 `pkg/moego` 里的 `Version` 改成即将发布的 tag，提交后打 tag：`v2.2.0-moego.N`（N 从 1 开始递增）。
5. 更新本文件的补丁清单。

## 同步上游

- 不自动同步。上游发布新版本后，把 `moego/main` rebase 到新版本，去掉已被上游合入的补丁，重跑验收用例后再发版。
- 上游正式版包含全部补丁时，评估退役本 fork。
