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

//#event-sourced-actor
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.rbmhtechnology.eventuate.AbstractEventsourcedActor;
import com.rbmhtechnology.eventuate.ReplicationConnection;
import com.rbmhtechnology.eventuate.ResultHandler;
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static akka.actor.ActorRef.noSender;
import static java.lang.System.out;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;

class ExampleActor extends AbstractEventsourcedActor {
  private Messages msgs = new Messages(); // outer class reference
  private final Optional<String> aggregateId;

  private Collection<String> currentState = Collections.emptyList();

  public ExampleActor(String id, Optional<String> aggregateId, ActorRef eventLog) {
    super(id, eventLog);
    this.aggregateId = aggregateId;

    setOnCommand(ReceiveBuilder
      .match(Messages.PrintCommand.class, cmd -> printState(id, currentState))
      .match(Messages.AppendCommand.class, cmd -> persist(msgs.new AppendedEvent(cmd.entry), ResultHandler.on(
        evt -> sender().tell(msgs.new AppendSuccessCommandReply(evt.entry), self()),
        err -> sender().tell(msgs.new AppendFailureCommandReply(err), self())
      )))
      .build());

    setOnEvent(ReceiveBuilder
      .match(Messages.AppendedEvent.class, evt -> currentState = append(currentState, evt.entry))
      .build());
  }

  @Override
  public Optional<String> getAggregateId() {
    return aggregateId;
  }

  private void printState(String id, Collection<String> currentState) {
    out.println(String.format("[id = %s, aggregate id = %s] %s", id, getAggregateId().orElseGet(() -> "undefined"),
      String.join(",", currentState)));
  }

  private <T> Collection<T> append(Collection<T> collection, T el) {
    return concat(collection.stream(), of(el)).collect(toList());
  }
}
//#

public class ActorExample {
  public static void main(String[] args) throws InterruptedException {

    //#create-one-instance
    Messages msgs = new Messages(); // outer class reference

    final ActorSystem system = // ...
      //#
      ActorSystem.create(ReplicationConnection.DefaultRemoteSystemName());

    //#create-one-instance
    final ActorRef eventLog = // ...
      //#
      system.actorOf(LeveldbEventLog.props("qt-1", "", false));


    //#create-one-instance

    final ActorRef ea1 = system.actorOf(Props.create(ExampleActor.class, () -> new ExampleActor("1", Optional.of("a"), eventLog)));

    ea1.tell(msgs.new AppendCommand("a"), noSender());
    ea1.tell(msgs.new AppendCommand("b"), noSender());
    //#

    //#print-one-instance
    ea1.tell(msgs.new PrintCommand(), noSender());
    //#

    //#create-two-instances
    final ActorRef b2 = system.actorOf(Props.create(ExampleActor.class, () -> new ExampleActor("2", Optional.of("b"), eventLog)));
    final ActorRef c3 = system.actorOf(Props.create(ExampleActor.class, () -> new ExampleActor("3", Optional.of("c"), eventLog)));

    b2.tell(msgs.new AppendCommand("a"), noSender());
    b2.tell(msgs.new AppendCommand("b"), noSender());

    c3.tell(msgs.new AppendCommand("x"), noSender());
    c3.tell(msgs.new AppendCommand("y"), noSender());
    //#

    //#print-two-instances
    b2.tell(msgs.new PrintCommand(), noSender());
    c3.tell(msgs.new PrintCommand(), noSender());
    //#

    //#create-replica-instances
    // created at location 1
    final ActorRef d4 = system.actorOf(Props.create(ExampleActor.class, () -> new ExampleActor("4", Optional.of("d"), eventLog)));

    // created at location 2
    final ActorRef d5 = system.actorOf(Props.create(ExampleActor.class, () -> new ExampleActor("5", Optional.of("d"), eventLog)));

    d4.tell(msgs.new AppendCommand("a"), noSender());
    //#

    Thread.sleep(1000);

    d4.tell(msgs.new PrintCommand(), noSender());
    d5.tell(msgs.new PrintCommand(), noSender());

    //#send-another-append
    d5.tell(msgs.new AppendCommand("b"), noSender());
    //#

    Thread.sleep(1000);

    d4.tell(msgs.new PrintCommand(), noSender());
    d5.tell(msgs.new PrintCommand(), noSender());
  }
}
