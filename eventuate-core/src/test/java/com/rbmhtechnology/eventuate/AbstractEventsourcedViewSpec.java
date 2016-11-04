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

package com.rbmhtechnology.eventuate;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Creator;
import akka.japi.pf.ReceiveBuilder;
import akka.testkit.TestProbe;
import com.rbmhtechnology.eventuate.EventsourcedViewSpec.Ping;
import com.rbmhtechnology.eventuate.EventsourcedViewSpec.Pong;
import com.rbmhtechnology.eventuate.EventsourcingProtocol.LoadSnapshot;
import com.rbmhtechnology.eventuate.EventsourcingProtocol.LoadSnapshotSuccess;
import com.rbmhtechnology.eventuate.EventsourcingProtocol.Replay;
import com.rbmhtechnology.eventuate.EventsourcingProtocol.ReplayFailure;
import com.rbmhtechnology.eventuate.EventsourcingProtocol.ReplaySuccess;
import javaslang.Tuple;
import org.junit.Before;
import org.junit.Test;
import scala.Option;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

public class AbstractEventsourcedViewSpec extends BaseSpec {

    public static class TestEventsourcedView extends AbstractEventsourcedView {
        public TestEventsourcedView(final String id, final ActorRef eventProbe, final ActorRef msgProbe) {
            super(id, eventProbe);

            setOnCommand(ReceiveBuilder
                    .match(Ping.class, p -> msgProbe.tell(new Pong(p.i()), self()))
                    .build());

            setOnEvent(ReceiveBuilder
                    .matchAny(ev -> msgProbe.tell(Tuple.of(ev, lastVectorTimestamp(), lastSequenceNr()), self()))
                    .build());

            setOnSnapshot(ReceiveBuilder
                    .matchAny(s -> msgProbe.tell("snapshot received", self()))
                    .build());
        }
    }

    public static class TestCompletionView extends AbstractEventsourcedView {
        public TestCompletionView(final String id, final ActorRef eventProbe, final ActorRef msgProbe) {
            super(id, eventProbe);

            setOnRecover(ResultHandler.on(
                    success -> msgProbe.tell("success", self()),
                    failure -> msgProbe.tell(failure, self())
            ));
        }
    }

    public static class TestBehaviourView extends AbstractEventsourcedView {
        public TestBehaviourView(final String id, final ActorRef eventProbe, final ActorRef msgProbe) {
            super(id, eventProbe);

            setOnCommand(ReceiveBuilder
                    .match(Ping.class, p -> msgProbe.tell(new Pong(p.i()), self()))
                    .matchEquals("become-ping", c -> commandContext().become(ping(msgProbe), false))
                    .matchEquals("unbecome", c -> commandContext().unbecome())
                    .build());
        }

        private PartialFunction<Object, BoxedUnit> ping(final ActorRef msgProbe) {
            return ReceiveBuilder
                    .match(Pong.class, p -> msgProbe.tell(new Ping(p.i()), self()))
                    .matchEquals("unbecome", c -> commandContext().unbecome())
                    .build();
        }
    }

    private Integer instanceId;
    private TestProbe logProbe;
    private TestProbe msgProbe;

    private DurableEvent event1a = createEvent("a", 1L);
    private DurableEvent event1b = createEvent("b", 2L);

    @Before
    public void beforeEach() {
        instanceId = getInstanceId();
        logProbe = new TestProbe(system);
        msgProbe = new TestProbe(system);
    }

    private ActorRef unrecoveredEventView() {
        return system.actorOf(Props.create(TestEventsourcedView.class,
                (Creator<TestEventsourcedView>) () -> new TestEventsourcedView(EMITTER_ID, logProbe.ref(), msgProbe.ref())));
    }

    private ActorRef recoveredEventView() {
        return processRecover(unrecoveredEventView(), instanceId);
    }

    private ActorRef unrecoveredCompletionView() {
        return system.actorOf(Props.create(TestCompletionView.class,
                (Creator<TestCompletionView>) () -> new TestCompletionView(EMITTER_ID, logProbe.ref(), msgProbe.ref())));
    }

    private ActorRef recoveredCompletionView() {
        return processRecover(unrecoveredCompletionView(), instanceId);
    }

    private ActorRef unrecoveredBehaviourView() {
        return system.actorOf(Props.create(TestBehaviourView.class,
                (Creator<TestBehaviourView>) () -> new TestBehaviourView(EMITTER_ID, logProbe.ref(), msgProbe.ref())));
    }

    private ActorRef recoveredBehaviourView() {
        return processRecover(unrecoveredBehaviourView(), instanceId);
    }

    private ActorRef processRecover(final ActorRef actor, final int instanceId) {
        return processRecover(actor, EMITTER_ID, instanceId, logProbe);
    }

    private Snapshot createSnapshot(final DurableEvent event) {
        return createSnapshot(EMITTER_ID, event);
    }

    @Test
    public void shouldRecoverFromReplayedEvents() {
        final ActorRef actor = unrecoveredEventView();

        logProbe.expectMsg(new LoadSnapshot(EMITTER_ID, instanceId));
        logProbe.sender().tell(new LoadSnapshotSuccess(Option.empty(), instanceId), logProbe.ref());
        logProbe.expectMsg(new Replay(1L, MAX_REPLAY_SIZE, Option.apply(actor), Option.empty(), instanceId));
        logProbe.sender().tell(new ReplaySuccess(toSeq(event1a, event1b), event1b.localSequenceNr(), instanceId), logProbe.ref());

        msgProbe.expectMsg(Tuple.of("a", event1a.vectorTimestamp(), event1a.localSequenceNr()));
        msgProbe.expectMsg(Tuple.of("b", event1b.vectorTimestamp(), event1b.localSequenceNr()));
    }

    @Test
    public void shouldRecoverFromSnapshot() {
        final ActorRef actor = unrecoveredEventView();

        logProbe.expectMsg(new LoadSnapshot(EMITTER_ID, instanceId));
        logProbe.sender().tell(new LoadSnapshotSuccess(Option.apply(createSnapshot(event1a)), instanceId), logProbe.ref());
        logProbe.expectMsg(new Replay(2L, MAX_REPLAY_SIZE, Option.apply(actor), Option.empty(), instanceId));
        logProbe.sender().tell(new ReplaySuccess(toSeq(event1a, event1b), event1b.localSequenceNr(), instanceId), logProbe.ref());

        msgProbe.expectMsg("snapshot received");
        msgProbe.expectMsg(Tuple.of("a", event1a.vectorTimestamp(), event1a.localSequenceNr()));
        msgProbe.expectMsg(Tuple.of("b", event1b.vectorTimestamp(), event1b.localSequenceNr()));
    }

    @Test
    public void shouldProcessCommandsAfterRecovery() {
        final ActorRef actor = recoveredEventView();

        actor.tell(new Ping(1), getRef());
        actor.tell(new Ping(2), getRef());
        actor.tell(new Ping(3), getRef());

        msgProbe.expectMsg(new Pong(1));
        msgProbe.expectMsg(new Pong(2));
        msgProbe.expectMsg(new Pong(3));
    }

    @Test
    public void shouldCallSuccessfulRecoverCompletionHandler() {
        recoveredCompletionView();
        msgProbe.expectMsg("success");
    }

    @Test
    public void shouldCallFailedRecoverCompletionHandler() {
        final ActorRef actor = unrecoveredCompletionView();

        logProbe.expectMsg(new LoadSnapshot(EMITTER_ID, instanceId));
        logProbe.sender().tell(new LoadSnapshotSuccess(Option.empty(), instanceId), logProbe.ref());
        logProbe.expectMsg(new Replay(1L, MAX_REPLAY_SIZE, Option.apply(actor), Option.empty(), instanceId));

        actor.tell(new ReplayFailure(FAILURE, 1L, instanceId), getRef());
        msgProbe.expectMsg(FAILURE);
    }

    @Test
    public void shouldChangeCommandBehaviour() {
        final ActorRef actor = recoveredBehaviourView();

        actor.tell(new Ping(1), getRef());
        msgProbe.expectMsg(new Pong(1));

        actor.tell("become-ping", getRef());
        actor.tell(new Pong(2), getRef());
        msgProbe.expectMsg(new Ping(2));

        actor.tell("unbecome", getRef());
        actor.tell(new Ping(3), getRef());
        msgProbe.expectMsg(new Pong(3));
    }

    @Test
    public void shouldKeepInitialBehaviourOnUnbecome() {
        final ActorRef actor = recoveredBehaviourView();

        actor.tell("unbecome", getRef());
        actor.tell(new Ping(1), getRef());
        msgProbe.expectMsg(new Pong(1));
    }
}
