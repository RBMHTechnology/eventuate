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

import com.google.protobuf.ByteString
import org.scalatest.Matchers
import org.scalatest.WordSpec

object BinaryPayloadManifestFilterSpec {
  def durableEventWithBinaryPayloadManifest(manifest: Option[String]): DurableEvent =
    DurableEvent(BinaryPayload(ByteString.EMPTY, 0, manifest, isStringManifest = true), "emitterId")
}

class BinaryPayloadManifestFilterSpec extends WordSpec with Matchers {

  import BinaryPayloadManifestFilterSpec._

  "BinaryPayloadManifestFilter" must {
    "pass BinaryPayloads with matching manifest" in {
      BinaryPayloadManifestFilter("a.*".r).apply(durableEventWithBinaryPayloadManifest(Some("abc"))) should be(true)
    }
    "filter BinaryPayloads with partially matching manifest" in {
      BinaryPayloadManifestFilter("b".r).apply(durableEventWithBinaryPayloadManifest(Some("abc"))) should be(false)
    }
    "filter BinaryPayloads with non-matching manifest" in {
      BinaryPayloadManifestFilter("a.*".r).apply(durableEventWithBinaryPayloadManifest(Some("bc"))) should be(false)
    }
    "filter BinaryPayloads without manifest" in {
      BinaryPayloadManifestFilter("a.*".r).apply(durableEventWithBinaryPayloadManifest(None)) should be(false)
    }
    "filter other payload" in {
      BinaryPayloadManifestFilter("a.*".r).apply(DurableEvent("payload", "emitterId")) should be(false)
    }
  }
}
