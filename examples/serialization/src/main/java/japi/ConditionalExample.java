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

package japi;

import static japi.DocUtils.append;
import japi.ViewExample.GetAppendCountCommandReply;

//#conditional-requests
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.rbmhtechnology.eventuate.*;
import scala.concurrent.ExecutionContextExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static scala.compat.java8.JFunction.func;
import static scala.compat.java8.JFunction.proc;
//#

public class ConditionalExample {
  //#conditional-requests

  class ExampleActor extends AbstractEventsourcedActor {
    private final String id;
    private Collection<String> currentState = Collections.emptyList();
    private Messages msgs = new Messages();

    public ExampleActor(String id, ActorRef eventLog) {
      super(id, eventLog);
      this.id = id;

      setOnCommand(ReceiveBuilder
        .match(Messages.AppendCommand.class, cmd -> persist(msgs.new AppendedEvent(cmd.entry), ResultHandler.onSuccess(
          evt -> sender().tell(msgs.new AppendSuccessWithTimestampCommandReply(evt.entry, lastVectorTimestamp()), self())
        )))
        // ...
        .build());

      setOnEvent(ReceiveBuilder
        .match(Messages.AppendedEvent.class, evt -> currentState = append(currentState, evt.entry))
        .build());
    }

    @Override
    public Optional<String> getAggregateId() {
      return Optional.of(id);
    }
  }

  // Eventsourced-View
  class ExampleView extends AbstractEventsourcedView {
    // AbstractEventsourcedView has ConditionalRequests mixed-in by default

    public ExampleView(String id, ActorRef eventLog) {
      super(id, eventLog);

      // ...
    }
  }
  //#
  public static void main(String[] args) {
    final ActorSystem system = ActorSystem.create("");
    final ActorRef eventLog = null;
    final ExecutionContextExecutor dispatcher = system.dispatcher();

    //#conditional-requests

    ConditionalExample ce = new ConditionalExample();
    Messages msgs = new Messages();
    final ActorRef ea = system.actorOf(Props.create(ExampleActor.class, () -> ce.new ExampleActor("ea", eventLog)));
    final ActorRef ev = system.actorOf(Props.create(ExampleView.class, () -> ce.new ExampleView("ev", eventLog)));

    final Timeout timeout = Timeout.apply(5, TimeUnit.SECONDS);

    Patterns.ask(ea, msgs.new AppendCommand("a"), timeout)
      .flatMap(func(m ->
          Patterns.ask(ev,
              new ConditionalRequest(((Messages.AppendSuccessWithTimestampCommandReply) m).updateTimestamp, ce.new GetAppendCount()), timeout)
          )
        , dispatcher)
      .onComplete(proc(result -> {
        if (result.isSuccess()) {
          System.out.println("append count = " + ((GetAppendCountCommandReply) result.get()).count);
        }
      }), dispatcher);

    //#
  }

  class GetAppendCount {}
}
