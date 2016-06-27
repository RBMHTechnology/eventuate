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

object VectorTimeApp extends App {
  val t1 = VectorTime("a" -> 1L, "b" -> 2L).dotted("a", 5L)
  val t2 = VectorTime("a" -> 1L, "b" -> 2L).dotted("a", 6L)
  val t3 = VectorTime("a" -> 6L, "b" -> 2L).dotted("a", 9L)
  val t4 = VectorTime("a" -> 6L, "b" -> 2L)

  val t10 = VectorTime("a" -> 1L, "b" -> 1L)
  val t11 = VectorTime("a" -> 3L, "b" -> 3L).dotted("a", 9L)

  val expectedVectorTimeE1 = VectorTime.Zero.dotted("b", 1L)
  val expectedVectorTimeE2 = VectorTime("b" -> 1L).dotted("a", 2L)

  val expectedVectorTimeE3 = VectorTime.Zero.dotted("b", 3L, 2L)
  val expectedVectorTimeE4 = VectorTime("b" -> 1L).dotted("b", 5L, 3L)

  val expectedVectorTimeE5 = VectorTime.Zero.dotted("b", 2L, 1L)
  val expectedVectorTimeE6 = VectorTime("b" -> 2L).dotted("a", 3L)

  val expectedVectorTimeE10 = VectorTime.Zero.dotted("a", 1L)
  val expectedVectorTimeE11 = VectorTime("a" -> 2L).dotted("a", 3L)

  println(expectedVectorTimeE1 <= expectedVectorTimeE2)
  println(expectedVectorTimeE5 <= expectedVectorTimeE6) // with gap in sequence

  println()
  println(t1 < t4)
  println(t4 > t1)
  println(t1 > t4)
  println(t1 merge t4)

  println()
  println(t1)
  println(t1.mergeDot)
  println(expectedVectorTimeE1)
  println(expectedVectorTimeE2)
  println(expectedVectorTimeE3)
  println(expectedVectorTimeE4)

  println()
  println(t1 <= t1)
  println(t1 equiv t1)

  println()
  println(t1 < t2)
  println(t1 <= t2)
  println(t1 conc t2)

  println()
  println(t2 < t3)
  println(t2 <= t3)
  println(t2 equiv t3)
  println(t2 conc t3)

  println()
  println(t10 < t11)

  println()
  println(expectedVectorTimeE10 <= expectedVectorTimeE11)
}
