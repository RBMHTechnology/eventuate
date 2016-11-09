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

import com.typesafe.config.ConfigFactory

import MultiNodeConfigCassandra._

class BasicReplicationSpecCassandra extends BasicReplicationSpec(new BasicReplicationConfig(providerConfig)) with MultiNodeSupportCassandra {
  override def logName = "br"
}
class BasicReplicationSpecCassandraMultiJvmNode1 extends BasicReplicationSpecCassandra
class BasicReplicationSpecCassandraMultiJvmNode2 extends BasicReplicationSpecCassandra
class BasicReplicationSpecCassandraMultiJvmNode3 extends BasicReplicationSpecCassandra

object BasicReplicationThroughputSpecCassandra {
  val config = ConfigFactory.parseString("eventuate.log.cassandra.index-update-limit = 200")
}
class BasicReplicationThroughputSpecCassandra extends BasicReplicationThroughputSpec(new BasicReplicationThroughputConfig(BasicReplicationThroughputSpecCassandra.config.withFallback(providerConfig))) with MultiNodeSupportCassandra {
  override def logName = "brt"
}
class BasicReplicationThroughputSpecCassandraMultiJvmNode1 extends BasicReplicationThroughputSpecCassandra
class BasicReplicationThroughputSpecCassandraMultiJvmNode2 extends BasicReplicationThroughputSpecCassandra
class BasicReplicationThroughputSpecCassandraMultiJvmNode3 extends BasicReplicationThroughputSpecCassandra
class BasicReplicationThroughputSpecCassandraMultiJvmNode4 extends BasicReplicationThroughputSpecCassandra
class BasicReplicationThroughputSpecCassandraMultiJvmNode5 extends BasicReplicationThroughputSpecCassandra
class BasicReplicationThroughputSpecCassandraMultiJvmNode6 extends BasicReplicationThroughputSpecCassandra

class BasicCausalitySpecCassandra extends BasicCausalitySpec(new BasicCausalityConfig(providerConfig)) with MultiNodeSupportCassandra {
  override def logName = "bc"
}
class BasicCausalitySpecCassandraMultiJvmNode1 extends BasicCausalitySpecCassandra
class BasicCausalitySpecCassandraMultiJvmNode2 extends BasicCausalitySpecCassandra

class BasicPersistOnEventSpecCassandra extends BasicPersistOnEventSpec(new BasicPersistOnEventConfig(providerConfig)) with MultiNodeSupportCassandra {
  override def logName = "bpe"
}
class BasicPersistOnEventSpecCassandraMultiJvmNode1 extends BasicPersistOnEventSpecCassandra
class BasicPersistOnEventSpecCassandraMultiJvmNode2 extends BasicPersistOnEventSpecCassandra

class FailureDetectionSpecCassandra extends FailureDetectionSpec(new FailureDetectionConfig(providerConfig)) with MultiNodeSupportCassandra {
  override def logName = "fd"
}
class FailureDetectionSpecCassandraMultiJvmNode1 extends FailureDetectionSpecCassandra
class FailureDetectionSpecCassandraMultiJvmNode2 extends FailureDetectionSpecCassandra

class FilteredReplicationSpecCassandra extends FilteredReplicationSpec(new FilteredReplicationConfig(providerConfig)) with MultiNodeSupportCassandra {
  override def logName = "fr"
}
class FilteredReplicationSpecCassandraMultiJvmNode1 extends FilteredReplicationSpecCassandra
class FilteredReplicationSpecCassandraMultiJvmNode2 extends FilteredReplicationSpecCassandra

