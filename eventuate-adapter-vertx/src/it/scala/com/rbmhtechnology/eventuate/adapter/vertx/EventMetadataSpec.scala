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

package com.rbmhtechnology.eventuate.adapter.vertx

import com.rbmhtechnology.eventuate.adapter.vertx.api.EventMetadata
import io.vertx.core.MultiMap
import org.scalatest.{ MustMatchers, WordSpecLike }

import scala.collection.JavaConverters._

class EventMetadataSpec extends WordSpecLike with MustMatchers {

  import EventMetadata._
  import EventMetadata.Headers._

  def headers(elems: (String, Any)*): MultiMap = {
    val headers = MultiMap.caseInsensitiveMultiMap()
    headers.setAll(Map(elems: _*).mapValues(_.toString).asJava)
    headers
  }

  "An EventMetadata" when {
    "supplied with valid headers" must {
      "be instantiated with all metadata" in {
        val metadata = EventMetadata.fromHeaders(headers(
          MessageProducer -> VertxProducer,
          LocalLogId -> "logA",
          LocalSequenceNr -> 1L,
          EmitterId -> "emitter1")
        )

        metadata.map(_.localLogId) mustBe Some("logA")
        metadata.map(_.localSequenceNr) mustBe Some(1L)
        metadata.map(_.emitterId) mustBe Some("emitter1")
      }
    }
    "supplied with invalid headers" must {
      "be empty if the source is not specified" in {
        val metadata = EventMetadata.fromHeaders(headers(
          LocalLogId -> "logA",
          LocalSequenceNr -> 1L,
          EmitterId -> "emitter1")
        )

        metadata mustBe None
      }
      "be empty if the headers are empty" in {
        val metadata = EventMetadata.fromHeaders(headers())

        metadata mustBe None
      }
      "fail to instantiate if the values have the wrong type" in {
        a[NumberFormatException] must be thrownBy EventMetadata.fromHeaders(headers(
          MessageProducer -> VertxProducer,
          LocalLogId -> "logA",
          LocalSequenceNr -> "i_am_not_a_long_value",
          EmitterId -> "emitter1")
        )
      }
      "fail to instantiate if the a value is missing" in {
        an[IllegalArgumentException] must be thrownBy EventMetadata.fromHeaders(headers(
          MessageProducer -> VertxProducer,
          LocalSequenceNr -> 1L,
          EmitterId -> "emitter1")
        )
      }
    }
  }
}
