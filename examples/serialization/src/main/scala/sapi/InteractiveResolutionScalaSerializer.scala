/*
 * Copyright 2015 - 2017 Red Bull Media House GmbH <http://www.redbullmediahouse.com> and Mike Slinn - all rights reserved.
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

//#scala-serializer
package sapi

import akka.serialization._

object InteractiveResolutionScalaSerializer {
  val UTF_8: String = java.nio.charset.StandardCharsets.UTF_8.name

  /* Case is significant for pattern matching to work. In case clauses, a term that begins with a
   * lowercase letter is assumed to be the name of a new variable that will hold an extracted value.
   * To refer to a previously defined variable, enclose it in back ticks. Conversely, a term that begins
   * with an uppercase letter is assumed to be a type name. */
  val AppendManifest: String         = classOf[InteractiveResolveExample.AppendCommand].getName         // "sapi.InteractiveResolveExample$Append"
  val AppendRejectedManifest: String = classOf[InteractiveResolveExample.AppendRejected].getName // "sapi.InteractiveResolveExample$AppendRejected"
  val ResolveManifest: String        = classOf[InteractiveResolveExample.ResolveCommand].getName        // "sapi.InteractiveResolveExample$Resolve"
  val ResolvedManifest: String       = classOf[InteractiveResolveExample.ResolvedEvent].getName       // "sapi.InteractiveResolveExample$Resolved"
}

class InteractiveResolutionScalaSerializer extends SerializerWithStringManifest {
  import InteractiveResolutionScalaSerializer._

  /** Unique identifier for your Serializer.
    * 0 - 16 is reserved by Akka itself */
  def identifier: Int = 1381719666

  /** The manifest (type hint) that will be provided in the fromBinary method.
    *  Use `""` if a manifest is not needed. */
  def manifest(obj: AnyRef): String =
    obj match {
      case _: InteractiveResolveExample.AppendCommand         => AppendManifest
      case _: InteractiveResolveExample.AppendRejected => AppendRejectedManifest
      case _: InteractiveResolveExample.ResolveCommand        => ResolveManifest
      case _: InteractiveResolveExample.ResolvedEvent       => ResolvedManifest
    }

  /** Serializes the given object to an Array[Byte] */
  def toBinary(obj: AnyRef): Array[Byte] =
    obj match {
      case InteractiveResolveExample.AppendCommand(entry) =>
        entry.getBytes(UTF_8)

      case appendRejected: InteractiveResolveExample.AppendRejected =>
        appendRejected.serialize

      case InteractiveResolveExample.ResolveCommand(vectorTime) =>
        vectorTime.serialize

      case InteractiveResolveExample.ResolvedEvent(vectorTime) =>
        vectorTime.serialize

      case _ => throw new java.io.NotSerializableException
  }

  /** Deserializes the given Array[Byte] using the type hint (`manifest`) */
  def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case AppendManifest =>
        InteractiveResolveExample.AppendCommand(new String(bytes, UTF_8))

      case AppendRejectedManifest =>
        InteractiveResolveExample.AppendRejected.deserialize(bytes)

      case ResolveManifest =>
        InteractiveResolveExample.ResolveCommand(RichVectorTime.deserialize(bytes))

      case ResolvedManifest =>
        InteractiveResolveExample.ResolvedEvent(RichVectorTime.deserialize(bytes))

      case _ => throw new java.io.NotSerializableException
    }
}
//#
