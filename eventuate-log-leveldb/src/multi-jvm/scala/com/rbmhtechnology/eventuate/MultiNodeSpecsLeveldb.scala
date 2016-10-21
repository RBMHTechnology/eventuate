/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate

import MultiNodeConfigLeveldb._

class BasicReplicationSpecLeveldb extends BasicReplicationSpec(new BasicReplicationConfig(providerConfig)) with MultiNodeSupportLeveldb
class BasicReplicationSpecLeveldbMultiJvmNode1 extends BasicReplicationSpecLeveldb
class BasicReplicationSpecLeveldbMultiJvmNode2 extends BasicReplicationSpecLeveldb
class BasicReplicationSpecLeveldbMultiJvmNode3 extends BasicReplicationSpecLeveldb

class BasicReplicationThroughputSpecLeveldb extends BasicReplicationThroughputSpec(new BasicReplicationThroughputConfig(providerConfig)) with MultiNodeSupportLeveldb
class BasicReplicationThroughputSpecLeveldbMultiJvmNode1 extends BasicReplicationThroughputSpecLeveldb
class BasicReplicationThroughputSpecLeveldbMultiJvmNode2 extends BasicReplicationThroughputSpecLeveldb
class BasicReplicationThroughputSpecLeveldbMultiJvmNode3 extends BasicReplicationThroughputSpecLeveldb
class BasicReplicationThroughputSpecLeveldbMultiJvmNode4 extends BasicReplicationThroughputSpecLeveldb
class BasicReplicationThroughputSpecLeveldbMultiJvmNode5 extends BasicReplicationThroughputSpecLeveldb
class BasicReplicationThroughputSpecLeveldbMultiJvmNode6 extends BasicReplicationThroughputSpecLeveldb

class BasicCausalitySpecLeveldb extends BasicCausalitySpec(new BasicCausalityConfig(providerConfig)) with MultiNodeSupportLeveldb
class BasicCausalitySpecLeveldbMultiJvmNode1 extends BasicCausalitySpecLeveldb
class BasicCausalitySpecLeveldbMultiJvmNode2 extends BasicCausalitySpecLeveldb

class BasicPersistOnEventSpecLeveldb extends BasicPersistOnEventSpec(new BasicPersistOnEventConfig(providerConfig)) with MultiNodeSupportLeveldb
class BasicPersistOnEventSpecLeveldbMultiJvmNode1 extends BasicPersistOnEventSpecLeveldb
class BasicPersistOnEventSpecLeveldbMultiJvmNode2 extends BasicPersistOnEventSpecLeveldb

class FailureDetectionSpecLeveldb extends FailureDetectionSpec(new FailureDetectionConfig(providerConfig)) with MultiNodeSupportLeveldb
class FailureDetectionSpecLeveldbMultiJvmNode1 extends FailureDetectionSpecLeveldb
class FailureDetectionSpecLeveldbMultiJvmNode2 extends FailureDetectionSpecLeveldb
