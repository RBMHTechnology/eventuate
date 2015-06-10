/*
 * Copyright (C) 2015 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

import java.util.{List => JList, Optional => JOption}
import java.util.function.BiFunction

import scala.collection.JavaConverters._
import scala.util._

import VersionedAggregate._

/**
 * Manages concurrent versions of an event-sourced aggregate.
 *
 * @param id Aggregate id
 * @param cmdHandler Command handler
 * @param evtHandler Event handler
 * @param aggregate Aggregate.
 * @tparam S Aggregate type.
 * @tparam C Command type.
 * @tparam E Event type.
 */
case class VersionedAggregate[S, C: DomainCmd, E: DomainEvt](
    id: String,
    cmdHandler: (S, C) => Try[E],
    evtHandler: (S, E) => S,
    aggregate: Option[ConcurrentVersions[S, E]] = None) {

  val C = implicitly[DomainCmd[C]]
  val E = implicitly[DomainEvt[E]]

  /**
   * Java API.
   */
  def getAggregate: JOption[ConcurrentVersions[S, E]] =
    JOption.ofNullable(aggregate.orNull)

  /**
   * Java API.
   */
  def getVersions: JList[Versioned[S]] =
    versions.asJava

  /**
   * Java API.
   */
  def doValidateCreate(cmd: C): E =
    validateCreate(cmd).get

  /**
   * Java API.
   */
  def doValidateUpdate(cmd: C): E =
    validateUpdate(cmd).get

  /**
   * Java API.
   */
  def doValidateResolve(selected: Int, origin: String): Resolved =
    validateResolve(selected, origin).get // FIXME

  def withAggregate(aggregate: ConcurrentVersions[S, E]): VersionedAggregate[S, C, E] =
    copy(aggregate = Some(aggregate))

  def versions: Seq[Versioned[S]] = aggregate match {
    case Some(versions) =>
      versions.all
    case None =>
      Seq.empty
  }

  def validateCreate(cmd: C): Try[E] = aggregate match {
    case None =>
      cmdHandler(null.asInstanceOf[S] /* FIXME */ , cmd)
    case Some(_) =>
      Failure(new AggregateAlreadyExistsException(id))
  }

  def validateUpdate(cmd: C): Try[E] = aggregate match {
    case None =>
      Failure(new AggregateDoesNotExistException(id))
    case Some(versions) if versions.conflict =>
      Failure(new ConflictDetectedException[S](id, versions.all))
    case Some(versions) =>
      cmdHandler(versions.all(0).value, cmd)
  }

  def validateResolve(selected: Int, origin: String): Try[Resolved] = aggregate match {
    case None =>
      Failure(new AggregateDoesNotExistException(id))
    case Some(versions) if !versions.conflict =>
      Failure(new ConflictNotDetectedException(id))
    case Some(versions) if versions.owner != origin =>
      Failure(new ConflictResolutionRejectedException(id, versions.owner, origin))
    case Some(versions) =>
      Success(Resolved(id, versions.all(selected).updateTimestamp, origin))
  }

  def handleCreated(evt: E, timestamp: VectorTime, sequenceNr: Long): VersionedAggregate[S, C, E] = {
    val versions = aggregate match {
      case None =>
        ConcurrentVersionsTree(evtHandler).withOwner(E.origin(evt))
      case Some(versions) => // concurrent create
        versions.withOwner(priority(versions.owner, E.origin(evt)))
    }
    copy(aggregate = Some(versions.update(evt, timestamp)))
  }

  def handleUpdated(evt: E, timestamp: VectorTime, sequenceNr: Long): VersionedAggregate[S, C, E] = {
    copy(aggregate = aggregate.map(_.update(evt, timestamp)))
  }

  def handleResolved(evt: Resolved, updateTimestamp: VectorTime, sequenceNr: Long): VersionedAggregate[S, C, E] = {
    copy(aggregate = aggregate.map(_.resolve(evt.selected, updateTimestamp)))
  }
}

object VersionedAggregate {
  trait DomainCmd[C] {
    def origin(cmd: C): String
  }

  trait DomainEvt[E] {
    def origin(evt: E): String
  }

  case class Resolved(id: String, selected: VectorTime, origin: String = "")
  case class Resolve(id: String, selected: Int, origin: String = "") {
    /** Java API */
    def withOrigin(origin: String) = copy(origin = origin)
  }

  class AggregateNotLoadedException(id: String)
    extends Exception(s"aggregate ${id} not loaded")

  class AggregateAlreadyExistsException(id: String)
    extends Exception(s"aggregate ${id} already exists")

  class AggregateDoesNotExistException(id: String)
    extends Exception(s"aggregate ${id} does not exist")

  class ConflictResolutionRejectedException(id: String, origin1: String, origin2: String)
    extends Exception(s"conflict for aggregate ${id} can only be resolved by ${origin1} but ${origin2} has attempted")

  class ConflictNotDetectedException(id: String)
    extends Exception(s"conflict for aggregate ${id} not detected")

  class ConflictDetectedException[S](id: String, val versions: Seq[Versioned[S]])
    extends Exception(s"conflict for aggregate ${id} detected") {

    /**
     * Java API.
     */
    def getVersions: JList[Versioned[S]] =
      versions.asJava
  }

  def create[S, C: DomainCmd, E: DomainEvt](
      id: String,
      cmdHandler: BiFunction[S, C, E],
      evtHandler: BiFunction[S, E, S]) =
    new VersionedAggregate[S, C, E](id,
      (s, c) => Try(cmdHandler.apply(s, c)),
      (s, e) => evtHandler.apply(s, e))

  def priority(creator1: String, creator2: String): String =
    if (creator1 < creator2) creator1 else creator2
}
