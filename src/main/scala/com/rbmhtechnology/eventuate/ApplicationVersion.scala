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

package com.rbmhtechnology.eventuate

object ApplicationVersion {
  implicit val orderedVersion = new Ordering[ApplicationVersion] {
    override def compare(x: ApplicationVersion, y: ApplicationVersion): Int =
      if (x.major == y.major) x.minor - y.minor else x.major - y.major
  }

  /**
   * Creates an `ApplicationVersion` from a version string of format `major.minor`.
   */
  def apply(version: String): ApplicationVersion =
    version match {
      case ApplicationVersion(major, minor) => ApplicationVersion(major, minor)
    }

  def unapply(s: String): Option[(Int, Int)] = {
    val mm = s.split("\\.")
    Some((mm(0).toInt, mm(1).toInt))
  }
}

/**
 * Defines an application version in terms of `major` version and `minor` version.
 */
case class ApplicationVersion(major: Int = 1, minor: Int = 0)
