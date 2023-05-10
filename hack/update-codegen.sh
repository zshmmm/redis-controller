#!/usr/bin/env bash

# Copyright 2017 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..

# 生成 deepcopy,client,informer,lister 资源框架脚本，可也按需调整
  # 框架代码输出位置 
  # api 资源代码输出位置
  # api group 版本
  # 代码根目录
  # 开源协议文件
"./bin/generate-groups.sh" "deepcopy,client,informer,lister" \
  redis/pkg/generated \
  redis/pkg/apis \
  qwoptcontroller:v1beta1 \
  --output-base "$(dirname "${BASH_SOURCE[0]}")/../.." \
  --go-header-file "${SCRIPT_ROOT}"/hack/boilerplate.go.txt

# To use your own boilerplate text append:
#   --go-header-file "${SCRIPT_ROOT}"/hack/custom-boilerplate.go.txt