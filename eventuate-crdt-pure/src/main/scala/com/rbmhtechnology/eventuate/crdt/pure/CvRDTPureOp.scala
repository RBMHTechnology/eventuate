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
import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.Versioned
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Redundancy_
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.SimpleCRDT
import com.rbmhtechnology.eventuate.crdt.pure.StabilityProtocol.TCStable

/**
 * Type-class for pure-op based CRDT.
 *
 * @tparam C CRDT state type
 * @tparam B CRDT value type
 */
trait CvRDTPureOp[C, B] extends CRDTServiceOps[CRDT[C], B] {

  /**
   * The data-type specific relations r,r0 and r1 used for reducing the POLog size via causal redundancy
   */
  implicit def causalRedundancy: CausalRedundancy

  /**
   * Data-type specific method that updates the stable state with the newly delivered operation, that
   * is always on the causal future of all the (non-timestamped) operations currently in the state.
   * This method is needed because operations in the causal future could make stable operations redundant (e.g. in a AWSet a [[ClearOp]] makes all the stable [[AddOp]] redundant and hence must be removed)
   * The returned state could potentially contain less operations if they were discarded, but it would never contain more operations, in other words the newly delivered operation must not be added to the state.
   *
   * @see [[POLog.add]]
   * @param op        the newly delivered operation
   * @param redundant a flag indicating if the newly delivered operation was redundant or it was added to the POLog.
   * @param state     the current CRDT state
   * @return the updated state
   */
  protected def updateState(op: Operation, redundant: Boolean, state: C): C

  /**
   * Adds the operation to the POLog using the causal redundancy relations and also updates the state
   * with the newly delivered operation.
   *
   * @param crdt
   * @param op              newly delivered operation
   * @param vt              operation's timestamp
   * @param systemTimestamp operation's metadata
   * @param creator         operation's metadata
   * @return a copy of the CRDT with its POLog and state updated
   */
  final def effect(crdt: CRDT[C], op: Operation, vt: VectorTime, systemTimestamp: Long = 0L, creator: String = ""): CRDT[C] = {
    val versionedOp = Versioned(op, vt, systemTimestamp, creator)
    val (updatedPolog, redundant) = crdt.polog.add(versionedOp)
    val updatedState = updateState(op, redundant, crdt.state)
    crdt.copy(updatedPolog, updatedState)
  }

  /**
   * "This function takes a stable
   * timestamp t (fed by the TCSB middleware) and the full PO-Log s as input, and returns
   * a new PO-Log (i.e., a map), possibly discarding a set of operations at once."
   *
   * @param polog
   * @param stable
   * @return
   */
  protected def stabilize(polog: POLog, stable: TCStable): POLog = polog

  /**
   * Updates the current [[CRDT.state]] with the sequence of stable operations that were removed
   * from the POLog on [[POLog.stable]]
   * The implementation will vary depending on the CRDT's state type.
   *
   * @param state     the current CRDT state
   * @param stableOps the sequence of stable operations that were removed from the POLog
   * @return the updated state
   */
  protected def stabilizeState(state: C, stableOps: Seq[Operation]): C

  /**
   * "The stable
   * handler, on the other hand, invokes stabilize and then strips the timestamp (if the operation
   * has not been discarded by stabilize), by replacing a (t', o') pair that is present in the returned
   * PO-Log by (⊥, o')"
   * The actual implementation doesn't replace the timestamp with ⊥, instead it calls [[stabilizeState]] with
   * the current [[CRDT.state]] and the sequence of stable operations discarded from the [[POLog]].
   *
   * @param crdt   a crdt
   * @param tcstable a stable VectorTime fed by the middleware
   * @return a possible optimized crdt after discarding operations and removing timestamps
   */
  override def stable(crdt: CRDT[C], tcstable: TCStable) = {
    val (stabilizedPOLog, stableOps) = stabilize(crdt.polog, tcstable).stable(tcstable)
    val stabilizedState = stabilizeState(crdt.state, stableOps)
    crdt.copy(stabilizedPOLog, stabilizedState)
  }

  /**
   * A pure-op based doesn't need to check preconditions because the framework just
   * returns the unmodified operation on prepare
   */
  override def precondition: Boolean = false

}

/**
 * A base trait for pure-op based CRDTs wich state is a sequence of stable operations
 *
 * @tparam B CRDT value type
 */
trait CvRDTPureOpSimple[B] extends CvRDTPureOp[Seq[Operation], B] {

  /**
   * This VectorTime is used during state update (due to a newly operation being delivered).
   * With this non zero VectorTime we can assure that a causal redundancy relation will return true if it is applied
   * against the operations in the stable state, which have [[VectorTime.Zero]].
   *
   */
  private val NON_ZERO_VECTOR_TIME = VectorTime.Zero.increment("ST")

  final override def zero: SimpleCRDT = CRDT.zero

  /**
   * Adds the stable operations to the state of the CRDT
   *
   * @param state     the current state of the CRDT
   * @param stableOps the causaly stable operations
   * @return the updated state
   */
  override protected def stabilizeState(state: Seq[Operation], stableOps: Seq[Operation]): Seq[Operation] = state ++ stableOps

  /**
   * To simplify the eval method, it asociates the [[VectorTime.Zero]] to each stable operation and calls [[customEval]]
   * over the full sequence of operations, the ones from the POLog and the ones from the state.
   * Note: this has to be a sequence instead of a Set because now the VectorTime is not unique
   */
  override final def eval(crdt: SimpleCRDT): B = {
    val stableOps = crdt.state.map(op => Versioned(op, VectorTime.Zero))
    customEval(stableOps ++ crdt.polog.log)
  }

  /**
   * Data-type specific eval over the full Seq of timestamped operations.
   * Note that the Seq contains both the non-stable operation from the POLog and the stable operations (timestamped with [[VectorTime.Zero]]) from the state.
   *
   * @param ops the full sequence of ops, stable (with the [[VectorTime.Zero]]) and non-stable
   * @return the value of the crdt
   */
  protected def customEval(ops: Seq[Versioned[Operation]]): B

  /**
   * Allows to define a custom implementation with improved performance to update the state.
   * e.g. The AWCart implementation returns an empty state if the newly delivered operation is a [[ClearOp]]
   *
   * @return the updated state
   */
  def optimizedUpdateState: PartialFunction[(Operation, Seq[Operation]), Seq[Operation]] = PartialFunction.empty

  /**
   * The default implementation to update the state uses a [[CausalRedundancy]] relation.
   * Note that this implementation traverse the whole Seq of stable operations to check if they are redundant
   * @param redundancy the redundancy relation that must be used to prune the state of stable operations
   * @param stateAndOp a pair conformed by the current state of the CRDT and the newly delivered op
   * @return the updated state
   */
  private def defaultUpdateState(redundancy: Redundancy_)(stateAndOp: (Operation, Seq[Operation])) = {
    val (op, state) = stateAndOp
    val redundant = redundancy(Versioned(op, NON_ZERO_VECTOR_TIME))
    state.map(Versioned(_, VectorTime.Zero)) filterNot redundant map (_.value)
  }

  /**
   * By default it uses [[CausalRedundancy.r0]] (if the op was redundant and hence not added to the POLog) or [[CausalRedundancy.r1]] (it the op was added to the POLog)
   * to update the state, unless the ops contains an implementation of [[optimizedUpdateState]]
   *
   * @param op        the newly delivered operation
   * @param redundant a flag indicating if the newly delivered operation was redundant or it was added to the POLog.
   * @param state     the current CRDT state
   * @return the updated state
   */
  override final protected def updateState(op: Operation, redundant: Boolean, state: Seq[Operation]): Seq[Operation] =
    optimizedUpdateState.applyOrElse((op, state), defaultUpdateState(causalRedundancy.redundancyFilter(redundant)))

}
