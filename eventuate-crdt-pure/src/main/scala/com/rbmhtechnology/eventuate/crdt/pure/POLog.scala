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

import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.CausalRedundancy
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Operation
import com.rbmhtechnology.eventuate.Versioned
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Redundancy_
import com.rbmhtechnology.eventuate.crdt.pure.StabilityProtocol.TCStable

/**
 * A Partial Ordered Log which retains all invoked operations together with their timestamps.
 *
 * @param log the set of operations with its timestamp and optional metadata (i.e. systemTimestamp, creator)
 */
case class POLog(log: Set[Versioned[Operation]] = Set.empty) extends CRDTFormat {

  /**
   * "Prunes the PO-Log once an operation is causally delivered in the effect. The aim is to
   * keep the smallest number of PO-Log operations such that all queries return
   * the same result as if the full PO-Log was present. In particular, this method discards
   * operations from the PO-Log if they can be removed without impacting the output of query
   * operations"
   * These is called causal redundancy and it is one of the two mechanisms that conforms the semantic
   * compaction used by the framework to reduce the size of pure op-based CRDTs. The other one is causal
   * stabilization through [[POLog.stable]].
   *
   * @param ops   the set of operations that conform the POLog
   * @param newOp the new delivered operation
   * @param r     a function that receives a new operation o and returns a filter that returns true if an operation o' is redundant by o
   * @return the set of operations that conform the POLog and are not redundant by newOp
   */
  private def prune(ops: Set[Versioned[Operation]], newOp: Versioned[Operation], r: Redundancy_): Set[Versioned[Operation]] = {
    val redundant = r(newOp)
    ops filterNot redundant
  }

  /**
   * "A newly delivered operation (t, o) is added to the PO-Log if it is not redundant
   * by the PO-Log operations [...]. An existing operation x in the PO-Log is removed
   * if it is made redundant by (t, o)"
   *
   * @see [[CvRDTPureOp.updateState]]
   * @param op  the operation to add
   * @param red the data type specific relations for causal redundancy
   * @return a pair conformed by
   *         - a boolean indicating if the operation was added to the POLog (i.e. it wasn't redundant). This is used after in [[CvRDTPureOpSimple.updateState]] to know wich [[Redundancy_]] relation must use for update the state.
   *         - the resulting POLog after adding and pruning.
   *         Note that the operation received may not be present in the returned POLog if it was redundant
   */
  def add(op: Versioned[Operation])(implicit red: CausalRedundancy): (POLog, Boolean) = {
    val redundant = red.r(op, this)
    val updatedLog = if (redundant) log else log + op
    val r = red.redundancyFilter(redundant)
    (copy(prune(updatedLog, op, r)), redundant)
  }

  /**
   * Discards all the operations from the POLog that are less or equal than the received [[TCStable]]
   * and returns a pair with the updated POLog and the discarded (stable) operations.
   *
   * @param stable the stable [[TCStable]] delivered by the TCSB middleware
   * @return a pair conformed by the [[POLog]] with only the operations that are not stable
   *         at the received [[TCStable]], and the set of operations that are stable
   *         at the received [[TCStable]]
   */
  def stable(stable: TCStable): (POLog, Seq[Operation]) = {
    val (stableOps, nonStableOps) = log.foldLeft((Seq.empty[Operation], Seq.empty[Versioned[Operation]])) {
      case ((stOps, nonStOps), op) =>
        if (stable.isStable(op.vectorTimestamp)) (stOps :+ op.value, nonStOps)
        else (stOps, nonStOps :+ op)
    }
    (copy(log = nonStableOps.toSet), stableOps)
  }

}
