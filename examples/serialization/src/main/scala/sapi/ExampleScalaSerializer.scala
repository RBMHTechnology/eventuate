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

object ExampleScalaSerializer {
  val UTF_8: String = java.nio.charset.StandardCharsets.UTF_8.name

  /* Case is significant for pattern matching to work. In case clauses, a term that begins with a
   * lowercase letter is assumed to be the name of a new variable that will hold an extracted value.
   * To refer to a previously defined variable, enclose it in back ticks. Conversely, a term that begins
   * with an uppercase letter is assumed to be a type name. */
  val AppendManifest: String        = classOf[AppendCommand].getName             // "sapi.AppendCommand"
  val AppendedManifest: String      = classOf[AppendedEvent].getName             // "sapi.AppendedEvent"
  val AppendFailureManifest: String = classOf[AppendFailureCommandReply].getName // "sapi.AppendFailureCommandReply"
  val AppendSuccessManifest: String = classOf[AppendSuccessCommandReply].getName // "sapi.AppendSuccessCommandReply"
  val PrintManifest: String         = PrintCommand.getClass.getName              // "sapi.PrintCommand$" note the $ at the end
}

class ExampleScalaSerializer extends SerializerWithStringManifest {
  import ExampleScalaSerializer._

  /** Unique identifier for your Serializer.
    * 0 - 16 is reserved by Akka itself */
  def identifier: Int = 93478411

  /** The manifest (type hint) that will be provided in the fromBinary method.
    *  Use `""` if a manifest is not needed. */
  def manifest(obj: AnyRef): String =
    obj match {
      case _: AppendCommand             => AppendManifest
      case _: AppendedEvent             => AppendedManifest
      case _: AppendFailureCommandReply => AppendFailureManifest
      case _: AppendSuccessCommandReply => AppendSuccessManifest
      case PrintCommand                 => PrintManifest
    }

  /** Serializes the given object to an Array[Byte] */
  def toBinary(obj: AnyRef): Array[Byte] =
    obj match {
      case AppendCommand(name)             => name.getBytes(UTF_8)
      case AppendFailureCommandReply(name) => name.getMessage.getBytes(UTF_8)
      case AppendSuccessCommandReply(name) => name.getBytes(UTF_8)
      case AppendedEvent(name)             => name.getBytes(UTF_8)
      case PrintCommand                    => "".getBytes(UTF_8) // case objects have no payload
  }

  /** Deserializes the given Array[Byte] using the type hint (`manifest`) */
  def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case AppendManifest =>
        AppendCommand(new String(bytes, UTF_8))

      case AppendedManifest =>
        AppendCommand(new String(bytes, UTF_8))

      case AppendFailureManifest =>
        import scala.util.control.NoStackTrace
        AppendFailureCommandReply(new Throwable(new String(bytes, UTF_8)) with NoStackTrace)

      case AppendSuccessManifest =>
        AppendSuccessCommandReply(new String(bytes, UTF_8))

      case PrintManifest =>
        PrintCommand

      case _ => throw new java.io.NotSerializableException
    }
}
//#
