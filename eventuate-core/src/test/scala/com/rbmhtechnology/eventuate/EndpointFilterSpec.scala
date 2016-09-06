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

import com.rbmhtechnology.eventuate.EndpointFilters.sourceFilters
import com.rbmhtechnology.eventuate.EndpointFilters.targetAndSourceFilters
import com.rbmhtechnology.eventuate.EndpointFilters.targetFilters
import com.rbmhtechnology.eventuate.EndpointFilters.targetOverwritesSourceFilters
import com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter
import org.scalatest.Matchers
import org.scalatest.WordSpec

object EndpointFilterSpec {
  def newFilter: ReplicationFilter = new ReplicationFilter {
    override def apply(event: DurableEvent): Boolean = true
  }
  val targetFilter = newFilter
  val sourceFilter = newFilter

  val targetLogId = "targetLogId"
  val sourceLogName = "sourceLogName"
}

class EndpointFilterSpec extends WordSpec with Matchers {
  import EndpointFilterSpec._

  "EndpointFilters" must {
    "and source and target filters" in {
      val endpointFilters = targetAndSourceFilters(Map(targetLogId -> targetFilter), Map(sourceLogName -> sourceFilter))

      endpointFilters.filterFor(targetLogId, sourceLogName) should be(targetFilter and sourceFilter)
      endpointFilters.filterFor("", sourceLogName) should be(sourceFilter)
      endpointFilters.filterFor(targetLogId, "") should be(targetFilter)
      endpointFilters.filterFor("", "") should be(NoFilter)
    }
    "overwrite source by target filters" in {
      val endpointFilters = targetOverwritesSourceFilters(Map(targetLogId -> targetFilter), Map(sourceLogName -> sourceFilter))

      endpointFilters.filterFor(targetLogId, sourceLogName) should be(targetFilter)
      endpointFilters.filterFor("", sourceLogName) should be(sourceFilter)
      endpointFilters.filterFor(targetLogId, "") should be(targetFilter)
      endpointFilters.filterFor("", "") should be(NoFilter)
    }
    "use source filters only" in {
      val endpointFilters = sourceFilters(Map(sourceLogName -> sourceFilter))

      endpointFilters.filterFor(targetLogId, sourceLogName) should be(sourceFilter)
      endpointFilters.filterFor(targetLogId, "") should be(NoFilter)
    }
    "use target filters only" in {
      val endpointFilters = targetFilters(Map(targetLogId -> targetFilter))

      endpointFilters.filterFor(targetLogId, sourceLogName) should be(targetFilter)
      endpointFilters.filterFor("", sourceLogName) should be(NoFilter)
    }
  }
}
