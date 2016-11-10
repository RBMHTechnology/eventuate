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

import scala.util.matching.Regex

/**
 * A [[ReplicationFilter]] that can be used in combination with
 * [[com.rbmhtechnology.eventuate.serializer.DurableEventSerializerWithBinaryPayload]].
 *
 * It evaluates to `true` if the payload's manifest matches `regex`.
 */
case class BinaryPayloadManifestFilter(regex: Regex) extends ReplicationFilter {

  override def apply(event: DurableEvent): Boolean = event.payload match {
    case BinaryPayload(_, _, Some(regex(_*)), _) => true
    case _                                       => false
  }
}

object BinaryPayloadManifestFilter {

  /**
   * Creates a [[BinaryPayloadManifestFilter]] for the regex given in `pattern`.
   */
  def create(pattern: String): BinaryPayloadManifestFilter =
    BinaryPayloadManifestFilter(pattern.r)
}
