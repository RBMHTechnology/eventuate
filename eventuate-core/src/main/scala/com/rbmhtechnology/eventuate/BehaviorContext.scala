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

import akka.actor.Actor.Receive

/**
 * Provides a context for managing behaviors.
 */
trait BehaviorContext {
  /**
   * The initial behavior.
   */
  def initial: Receive

  /**
   * The current behavior.
   */
  def current: Receive

  /**
   * Sets the given `behavior` as `current` behavior.
   *
   * @param behavior the behavior to set.
   * @param replace if `true` (default) replaces the `current` behavior on the behavior stack, if `false`
   *                pushes `behavior` on the behavior stack.
   */
  def become(behavior: Receive, replace: Boolean = true): Unit

  /**
   * Pops the current `behavior` from the behavior stack, making the previous behavior the `current` behavior.
   * If the behavior stack contains only a single element, the `current` behavior is reverted to the `initial`
   * behavior.
   */
  def unbecome(): Unit
}

private class DefaultBehaviorContext(val initial: Receive) extends BehaviorContext {
  private var behaviorStack: List[Receive] = List(initial)

  override def current: Receive =
    behaviorStack.head

  override def become(behavior: Receive, discardOld: Boolean): Unit = {
    behaviorStack = behaviorStack match {
      case b :: bs if discardOld => behavior :: bs
      case bs                    => behavior :: bs
    }
  }

  override def unbecome(): Unit = {
    behaviorStack = behaviorStack match {
      case b :: Nil => initial :: Nil
      case bs       => bs.tail
    }
  }
}

private class StaticBehaviorContext(val initial: Receive) extends BehaviorContext {
  override def current: Receive =
    initial

  override def become(behavior: Receive, discardOld: Boolean): Unit =
    throw new UnsupportedOperationException("Behavior change not supported")

  override def unbecome(): Unit =
    ()
}
