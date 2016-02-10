/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.example.dbreplica.event

import java.util.UUID

sealed trait AssetEvent {
  def assetId: String
}

object AssetCreated {
  def apply(subject: String, content: String): AssetCreated =
    new AssetCreated(UUID.randomUUID().toString, subject, content)
}

case class AssetCreated(assetId: String, subject: String, content: String) extends AssetEvent
case class AssetSubjectUpdated(assetId: String, subject: String) extends AssetEvent
case class AssetContentUpdated(assetId: String, content: String) extends AssetEvent

