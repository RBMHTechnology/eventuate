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

//#event-sourced-view

package japi;

import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import com.rbmhtechnology.eventuate.AbstractEventsourcedView;
import com.rbmhtechnology.eventuate.VectorTime;
//#

// TODO turn this into a runnable example
public class ViewExample {
  //#event-sourced-view

  class ExampleView extends AbstractEventsourcedView {

    private Long appendCount = 0L;
    private Long resolveCount = 0L;

    public ExampleView(String id, ActorRef eventLog) {
      super(id, eventLog);

      setOnCommand(ReceiveBuilder
        .match(GetAppendCount.class, cmd -> sender().tell(new GetAppendCountReply(appendCount), self()))
        .match(GetResolveCount.class, cmd -> sender().tell(new GetResolveCountReply(resolveCount), self()))
        .build());

      setOnEvent(ReceiveBuilder
        .match(Appended.class, evt -> appendCount += 1)
        .match(Resolved.class, evt -> resolveCount += 1)
        .build());
    }
  }

  // Commands
  class GetAppendCount { }

  class GetResolveCount { }

  // Command replies
  class GetAppendCountReply {
    public final Long count;

    public GetAppendCountReply(Long count) {
      this.count = count;
    }
  }

  class GetResolveCountReply {
    public final Long count;

    public GetResolveCountReply(Long count) {
      this.count = count;
    }
  }

  // Events
  class Appended {
    public final String entry;

    public Appended(String entry) {
      this.entry = entry;
    }
  }

  class Resolved {
    public final VectorTime selectedTimestamp;

    public Resolved(VectorTime selectedTimestamp) {
      this.selectedTimestamp = selectedTimestamp;
    }
  }
  //#
}
