/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

package com.rbmhtechnology.eventuate;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Creator;
import akka.japi.pf.ReceiveBuilder;
import akka.testkit.TestProbe;
import com.rbmhtechnology.eventuate.EventsourcedActorSpec.Cmd;
import com.rbmhtechnology.eventuate.EventsourcingProtocol.Write;
import com.rbmhtechnology.eventuate.EventsourcingProtocol.WriteFailure;
import com.rbmhtechnology.eventuate.EventsourcingProtocol.WriteSuccess;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AbstractEventsourcedActorSpec extends BaseSpec {

    public static class TestEventsourcedActor extends AbstractEventsourcedActor {

        public TestEventsourcedActor(final String id, final ActorRef logProbe, final ActorRef mgsProbe) {
            super(id, logProbe);

            setOnCommand(ReceiveBuilder
                    .match(Cmd.class, cmd -> cmd.num() == 1, cmd -> persist(cmd.payload(), ResultHandler.on(
                            success -> mgsProbe.tell(cmd.payload(), self()),
                            failure -> mgsProbe.tell(failure, self())
                    )))
                    .match(Cmd.class, cmd -> persistN(createN(cmd.payload(), cmd.num()), ResultHandler.on(
                            success -> mgsProbe.tell(cmd.payload(), self()),
                            failure -> mgsProbe.tell(failure, self())
                    )))
                    .build());
        }

        private Collection<String> createN(final Object payload, final int cnt) {
            return IntStream.rangeClosed(1, cnt).boxed().map(i -> payload + "-" + i).collect(toList());
        }
    }

    private Integer instanceId;
    private TestProbe logProbe;
    private TestProbe msgProbe;

    @Before
    public void beforeEach() {
        instanceId = getInstanceId();
        logProbe = new TestProbe(system);
        msgProbe = new TestProbe(system);
    }

    private ActorRef unrecoveredEventActor() {
        return system.actorOf(Props.create(TestEventsourcedActor.class,
                (Creator<TestEventsourcedActor>) () -> new TestEventsourcedActor(EMITTER_ID, logProbe.ref(), msgProbe.ref())));
    }

    private ActorRef recoveredEventActor() {
        return processRecover(unrecoveredEventActor(), EMITTER_ID, instanceId, logProbe);
    }

    @Test
    public void shouldPersistSingleEvent() {
        final ActorRef actor = recoveredEventActor();

        actor.tell(new Cmd("a", 1), getRef());

        final Write write = logProbe.expectMsgClass(Write.class);
        assertEquals("a", write.events().head().payload());

        logProbe.sender().tell(new WriteSuccess(toSeq(createEvent("a", 1L)), write.correlationId(), instanceId), logProbe.ref());

        msgProbe.expectMsg("a");
    }

    @Test
    public void shouldPersistMultipleEvents() {
        final ActorRef actor = recoveredEventActor();

        actor.tell(new Cmd("a", 3), getRef());

        final Write write = logProbe.expectMsgClass(Write.class);
        assertEquals("a-1", write.events().head().payload());
        assertEquals("a-2", write.events().apply(1).payload());
        assertEquals("a-3", write.events().apply(2).payload());

        logProbe.sender().tell(new WriteSuccess(toSeq(createEvent("a-1", 1L), createEvent("a-2", 2L), createEvent("a-3", 3L)),
                write.correlationId(), instanceId), logProbe.ref());

        msgProbe.expectMsg("a");
    }

    @Test
    public void shouldHandleWriteFailure() {
        final ActorRef actor = recoveredEventActor();

        actor.tell(new Cmd("a", 1), getRef());

        final Write write = logProbe.expectMsgClass(Write.class);
        assertEquals("a", write.events().head().payload());

        logProbe.sender().tell(new WriteFailure(toSeq(createEvent("a", 1L)), FAILURE, write.correlationId(), instanceId), logProbe.ref());

        msgProbe.expectMsg(FAILURE);
    }
}
