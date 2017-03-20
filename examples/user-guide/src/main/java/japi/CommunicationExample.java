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

//#event-driven-communication

package japi;

//#event-driven-communication1
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.rbmhtechnology.eventuate.AbstractEventsourcedActor;
import com.rbmhtechnology.eventuate.ResultHandler;
import static akka.actor.ActorRef.noSender;

//#

public class CommunicationExample {
  //#ping-actor

  class PingActor extends AbstractEventsourcedActor {
    public PingActor(String id, ActorRef eventLog, ActorRef completion) {
      super(id, eventLog);

      setOnCommand(ReceiveBuilder
        .matchEquals("serve", cmd -> persist(new PingEvent(1), ResultHandler.none()))
        .build());

      setOnEvent(ReceiveBuilder
        .match(PongEvent.class, evt -> evt.num == 10 && !recovering(), evt -> completion.tell("done", self()))
        .match(PongEvent.class, evt -> persistOnEvent(new PingEvent(evt.num + 1)))
        .build());
    }
  }
  //#
  //#pong-actor

  class PongActor extends AbstractEventsourcedActor {
    public PongActor(String id, ActorRef eventLog) {
      super(id, eventLog);

      setOnEvent(ReceiveBuilder
        .match(PingEvent.class, evt -> persistOnEvent(new PongEvent(evt.num)))
        .build());
    }
  }

  //#ping-pong-events

  class PingEvent {
    public final Integer num;

    public PingEvent(Integer num) {
      this.num = num;
    }
  }

  class PongEvent {
    public final Integer num;

    public PongEvent(Integer num) {
      this.num = num;
    }
  }
  //#

  public static void main(String[] args) {
    //#event-driven-communication1
    final ActorSystem system = ActorSystem.create("system");
    final ActorRef eventLog = null;
    //#
    //#event-driven-communication2

    CommunicationExample ce = new CommunicationExample();
    final ActorRef pingActor = system.actorOf(
        Props.create(
            PingActor.class,
            () -> ce.new PingActor("ping", eventLog, system.deadLetters())
        )
    );
    final ActorRef pongActor = system.actorOf(
        Props.create(
            PongActor.class,
            () -> ce.new PongActor("pong", eventLog)
        )
    );

    pingActor.tell("serve", noSender());
    //#
  }
}
