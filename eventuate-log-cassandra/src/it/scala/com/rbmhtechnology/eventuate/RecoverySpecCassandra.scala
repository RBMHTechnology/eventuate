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

import com.rbmhtechnology.eventuate.ReplicationIntegrationSpec.replicationConnection
import com.rbmhtechnology.eventuate.utilities._
import org.scalatest.Matchers
import org.scalatest.WordSpec

class RecoverySpecCassandra extends WordSpec with Matchers with MultiLocationSpecCassandra {
  "ReplicationEndpoint recovery" must {
    "leave the index in an consistent state" in { // test for issue #393
      def newLocationA = location("A")
      val locationA1 = newLocationA
      val locationB = location("B")

      def newEndpointA(l: Location) = l.endpoint(Set("L1"), Set(replicationConnection(locationB.port)), activate = false)
      val endpointA1 = newEndpointA(locationA1)
      locationB.endpoint(Set("L1"), Set(replicationConnection(locationA1.port)))

      val logA = endpointA1.target("L1")
      write(logA, List("1"), Some("A1"))

      endpointA1.recover().await
      locationA1.terminate().await

      val locationA2 = newLocationA
      val endpointA2 = newEndpointA(locationA2)
      locationA2.listener(endpointA2.logs("L1"), Some("A1")).waitForMessage("1")
    }
  }

}
