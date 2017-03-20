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

//#event-sourced-actor
package japi;

import com.rbmhtechnology.eventuate.VectorTime;

/** Outer class allows many inner classes to be defined in one file */
public class Messages {
  public class PrintCommand {}

  public class AppendCommand {
    public final String entry;

    public AppendCommand(String entry) {
      this.entry = entry;
    }
  }

  public class AppendSuccessCommandReply {
    public final String entry;

    public AppendSuccessCommandReply(String entry) {
      this.entry = entry;
    }
  }

  public class AppendFailureCommandReply {
    public final Throwable cause;

    public AppendFailureCommandReply(Throwable cause) {
      this.cause = cause;
    }
  }

  public class AppendedEvent {
    public final String entry;

    public AppendedEvent(String entry) {
      this.entry = entry;
    }
  }
//#
//#conditional-requests
  public class AppendSuccessWithTimestampCommandReply {
    public final String entry;
    public final VectorTime updateTimestamp;

    public AppendSuccessWithTimestampCommandReply(String entry, VectorTime updateTimestamp) {
      this.entry = entry;
      this.updateTimestamp = updateTimestamp;
    }
  }
//#
//#event-sourced-actor
}
//#
