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

import java.util.{List => JList, Map => JMap}
import java.util.function.BiFunction

import scala.collection.JavaConverters._
import scala.util._

import VersionedObjects._

/**
 * An implementation of the (invented) versioned domain objects pattern.
 */
class VersionedObjects[S, C: DomainCmd, E: DomainEvt](
    cmdHandler: (S, C) => Try[E],
    evtHandler: (S, E) => S) {

  private var _current: Map[String, ConcurrentVersions[S, E]] = Map.empty

  val C = implicitly[DomainCmd[C]]
  val E = implicitly[DomainEvt[E]]

  def current =
    _current

  def versions(id: String): Seq[Versioned[S]] = current.get(id) match {
    case Some(versions) => versions.all
    case None           => Seq.empty
  }

  /**
   * Java API.
   */
  def getCurrent(): JMap[String, ConcurrentVersions[S, E]] =
    current.asJava

  /**
   * Java API.
   */
  def getVersions(id: String): JList[Versioned[S]] =
    versions(id).asJava

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
  def doValidateResolve(cmd: Resolve): Resolved =
    validateResolve(cmd).get

  def validateCreate(cmd: C): Try[E] = current.get(C.id(cmd)) match {
    case None =>
      cmdHandler(null.asInstanceOf[S], cmd)
    case Some(_) =>
      Failure(new ObjectAlreadyExistsException(C.id(cmd)))
  }

  def validateUpdate(cmd: C): Try[E] = current.get(C.id(cmd)) match {
    case None =>
      Failure(new ObjectDoesNotExistException(C.id(cmd)))
    case Some(versions) if versions.conflict =>
      Failure(new ConflictDetectedException[S](C.id(cmd), versions.all))
    case Some(versions) =>
      cmdHandler(versions.all(0).value, cmd)
  }

  def validateResolve(cmd: Resolve): Try[Resolved] = current.get(cmd.id) match {
    case None =>
      Failure(new ObjectDoesNotExistException(cmd.id))
    case Some(versions) if !versions.conflict =>
      Failure(new ConflictNotDetectedException(cmd.id))
    case Some(versions) if versions.owner != cmd.origin =>
      Failure(new ConflictResolutionRejectedException(cmd.id, versions.owner, cmd.origin))
    case Some(versions) =>
      Success(Resolved(cmd.id, versions.all(cmd.selected).version, cmd.origin))
  }

  def handleCreated(evt: E, timestamp: VectorTime, sequenceNr: Long): Unit = {
    val versions = current.get(E.id(evt)) match {
      case None =>
        ConcurrentVersionsTree(evtHandler).withOwner(E.origin(evt))
      case Some(versions) => // concurrent create
        versions.withOwner(priority(versions.owner, E.origin(evt)))
    }
    _current += (E.id(evt) -> versions.update(evt, timestamp))
  }

  def handleUpdated(evt: E, timestamp: VectorTime, sequenceNr: Long): Unit = {
    current.get(E.id(evt)) match {
      case None => // ignore (can only happen with async persist)
      case Some(versions) =>
        _current += (E.id(evt) -> versions.update(evt, timestamp))
    }
  }

  def handleResolved(evt: Resolved, timestamp: VectorTime, sequenceNr: Long): Unit = {
    val versions = current.get(evt.id) match {
      case None => // ignore (can only happen with async persist)
      case Some(versions) =>
        _current += (evt.id -> versions.resolve(evt.selected, timestamp))
    }
  }
}

object VersionedObjects {
  trait DomainCmd[C] {
    def id(cmd: C): String
    def origin(cmd: C): String
  }

  trait DomainEvt[E] {
    def id(evt: E): String
    def origin(evt: E): String
  }

  case class Resolved(id: String, selected: VectorTime, origin: String = "")
  case class Resolve(id: String, selected: Int, origin: String = "") {
    /** Java API */
    def withOrigin(origin: String) = copy(origin = origin)
  }

  class ObjectNotLoadedException(id: String)
    extends Exception(s"object ${id} not loaded")

  class ObjectAlreadyExistsException(id: String)
    extends Exception(s"object ${id} already exists")

  class ObjectDoesNotExistException(id: String)
    extends Exception(s"object ${id} does not exist")

  class ConflictResolutionRejectedException(id: String, origin1: String, origin2: String)
    extends Exception(s"conflict for object ${id} can only be resolved by ${origin1} but ${origin2} has attempted")

  class ConflictNotDetectedException(id: String)
    extends Exception(s"conflict for object ${id} not detected")

  class ConflictDetectedException[S](id: String, val versions: Seq[Versioned[S]])
    extends Exception(s"conflict for object ${id} detected") {

    /**
     * Java API.
     */
    def getVersions: JList[Versioned[S]] =
      versions.asJava
  }

  def priority(creator1: String, creator2: String): String =
    if (creator1 < creator2) creator1 else creator2

  def create[S, C: DomainCmd, E: DomainEvt](
      cmdHandler: BiFunction[S, C, E],
      evtHandler: BiFunction[S, E, S]) =
    new VersionedObjects[S, C, E]((s, c) => Try(cmdHandler.apply(s, c)), (s, e) => evtHandler.apply(s, e))
}
