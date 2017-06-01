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

package userguide.japi;

//#event-driven-communication
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.rbmhtechnology.eventuate.AbstractEventsourcedActor;
import com.rbmhtechnology.eventuate.ResultHandler;
import static akka.actor.ActorRef.noSender;
//#

public class CommunicationExample {

  //#event-driven-communication

  class PingActor extends AbstractEventsourcedActor {

    private final ActorRef completion;

    public PingActor(String id, ActorRef eventLog, ActorRef completion) {
      super(id, eventLog);
      this.completion = completion;
    }

    @Override
    public AbstractActor.Receive createOnCommand() {
      return receiveBuilder()
          .matchEquals("serve", cmd -> persist(new Ping(1), ResultHandler.none()))
          .build();
    }

    @Override
    public AbstractActor.Receive createOnEvent() {
      return receiveBuilder()
          .match(Pong.class, evt -> evt.num == 10 && !isRecovering(), evt -> completion.tell("done", getSelf()))
          .match(Pong.class, evt -> persistOnEvent(new Ping(evt.num + 1)))
          .build();
    }
  }

  class PongActor extends AbstractEventsourcedActor {

    public PongActor(String id, ActorRef eventLog) {
      super(id, eventLog);
    }

    @Override
    public AbstractActor.Receive createOnEvent() {
      return receiveBuilder()
          .match(Ping.class, evt -> persistOnEvent(new Pong(evt.num)))
          .build();
    }
  }

  class Ping {
    public final Integer num;

    public Ping(Integer num) {
      this.num = num;
    }
  }

  class Pong {
    public final Integer num;

    public Pong(Integer num) {
      this.num = num;
    }
  }
  //#

  public void main() {
    final ActorSystem system = ActorSystem.create("system");
    final ActorRef eventLog = null;

    //#event-driven-communication

    final ActorRef pingActor = system.actorOf(Props.create(PingActor.class, () -> new PingActor("ping", eventLog, system.deadLetters())));
    final ActorRef pongActor = system.actorOf(Props.create(PongActor.class, () -> new PongActor("pong", eventLog)));

    pingActor.tell("serve", noSender());
    //#
  }
}
