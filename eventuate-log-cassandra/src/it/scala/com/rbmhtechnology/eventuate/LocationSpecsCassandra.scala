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

import akka.actor.{ Props, ActorSystem }
import akka.testkit.TestKit

// --------------------------------------------------------------------------
//  Provider-specific single-location specs
// --------------------------------------------------------------------------

class EventsourcedProcessorIntegrationSpecCassandra extends TestKit(ActorSystem("test")) with EventsourcedProcessorIntegrationSpec with SingleLocationSpecCassandra {
  override def beforeEach(): Unit = {
    super.beforeEach()
    init()
  }
}

class EventsourcedActorIntegrationSpecCassandra extends TestKit(ActorSystem("test")) with EventsourcedActorIntegrationSpec with SingleLocationSpecCassandra {
  override def batching = false
}

class PersistOnEventIntegrationSpecCassandra extends TestKit(ActorSystem("test")) with PersistOnEventIntegrationSpec with SingleLocationSpecCassandra
class EventsourcedActorThroughputSpecCassandra extends TestKit(ActorSystem("test")) with EventsourcedActorThroughputSpec with SingleLocationSpecCassandra

// --------------------------------------------------------------------------
//  Provider-specific multi-location specs
// --------------------------------------------------------------------------

class EventsourcedActorCausalitySpecCassandra extends EventsourcedActorCausalitySpec with MultiLocationSpecCassandra {
  override val logFactory: String => Props = id => SingleLocationSpecCassandra.TestEventLog.props(id, batching = true)
}

class ReplicationIntegrationSpecCassandra extends ReplicationIntegrationSpec with MultiLocationSpecCassandra {
  def customPort = 2554
}

class ReplicationCycleSpecCassandra extends ReplicationCycleSpec with MultiLocationSpecCassandra
