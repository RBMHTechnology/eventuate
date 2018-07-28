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

package com.rbmhtechnology.eventuate.crdt.pure

import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Operation

object CRDT {

  def apply[A](state: A): CRDT[A] = CRDT(POLog(), state)

  def zero: CRDT[Seq[Operation]] = CRDT(POLog(), Seq.empty)
}

/**
 * A pure op-based CRDT wich state is splitted in two componentes. A map of timestamps to operations (the POLog) and
 * a plain set of stable operations or a specialized implementation (the state)
 * P(O) × (T ֒→ O)
 *
 * @param polog The POLog contains only the set of timestamped operations
 * @param state The state of the CRDT that contains stable operations (non-timestamped) or a "specialized
 *              implementation according to the domain" e.g., a bitmap for dense sets of integers in an AWSet
 * @tparam B state type
 */
case class CRDT[B](polog: POLog, state: B) extends CRDTFormat
