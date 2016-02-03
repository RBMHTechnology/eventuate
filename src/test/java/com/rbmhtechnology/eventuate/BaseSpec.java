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
import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import akka.testkit.TestProbe;
import com.rbmhtechnology.eventuate.EventsourcingProtocol.LoadSnapshot;
import com.rbmhtechnology.eventuate.EventsourcingProtocol.LoadSnapshotSuccess;
import com.rbmhtechnology.eventuate.EventsourcingProtocol.Replay;
import com.rbmhtechnology.eventuate.EventsourcingProtocol.ReplaySuccess;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import scala.Option;
import scala.collection.immutable.$colon$colon;
import scala.collection.immutable.List$;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Set$;
import scala.collection.immutable.Vector$;

import java.util.Arrays;

public abstract class BaseSpec {

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    public static final Throwable FAILURE = new NoStackTraceException("failure");
    public static final int MAX_REPLAY_SIZE = 4096;
    public static final String EMITTER_ID = "emitterId";
    public static final String LOG_ID = "logId";

    protected static ActorSystem system;

    protected TestProbe testProbe;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        JavaTestKit.shutdownActorSystem(system);
        system = null;
    }

    @Before
    public void beforeEachBase() {
        testProbe = new TestProbe(system);
    }

    protected ActorRef getRef() {
        return testProbe.ref();
    }

    protected int getInstanceId() {
        return EventsourcedView$.MODULE$.instanceIdCounter().get();
    }

    protected ActorRef processRecover(final ActorRef actor, final String emitterId, final int instanceId, final TestProbe logProbe) {
        return processRecover(actor, emitterId, instanceId, logProbe, MAX_REPLAY_SIZE);
    }

    protected ActorRef processRecover(final ActorRef actor, final String emitterId, final int instanceId, final TestProbe logProbe,
                                      final int replaySize, final DurableEvent... events) {
        logProbe.expectMsg(new LoadSnapshot(emitterId, instanceId));
        logProbe.sender().tell(new LoadSnapshotSuccess(Option.empty(), instanceId), logProbe.ref());

        processReplay(actor, 1L, instanceId, logProbe, replaySize, events);
        return actor;
    }

    protected ActorRef processReplay(final ActorRef actor, final long fromSequenceNr, final int instanceId, final TestProbe logProbe,
                                     final int replaySize, final DurableEvent... events) {
        logProbe.expectMsg(new Replay(fromSequenceNr, replaySize, Option.apply(actor), Option.empty(), instanceId));
        actor.tell(new ReplaySuccess(toSeq(events), 0L, instanceId), logProbe.ref());
        return actor;
    }

    protected Snapshot createSnapshot(final String emitterId, final DurableEvent event) {
        return new Snapshot(event, emitterId, event, VectorTime.apply(toSeq()),
                Vector$.MODULE$.<ConfirmedDelivery.DeliveryAttempt>empty(), Vector$.MODULE$.<PersistOnEvent.PersistOnEventRequest>empty());
    }

    protected DurableEvent createEvent(final Object payload, final long sequenceNr) {
        return createEvent(payload, sequenceNr, EMITTER_ID, LOG_ID, timestamp(sequenceNr, 0));
    }

    @SuppressWarnings("unchecked")
    protected DurableEvent createEvent(final Object payload, final long sequenceNr, final String emitterId, final String logId, final VectorTime timestamp) {
        return new DurableEvent(payload, emitterId, Option.empty(), Set$.MODULE$.<String>empty(), 0L,
                timestamp, logId, logId, sequenceNr, Option.empty());
    }

    protected VectorTime timestamp(final long a, final long b) {
        return EventsourcedViewSpec$.MODULE$.timestamp(a, b);
    }

    @SafeVarargs
    protected static <T> Seq<T> toSeq(final T... elems) {
        return Arrays.stream(elems)
                .reduce(List$.MODULE$.<T>empty(), (acc, elem) -> new $colon$colon<>((T) elem, acc), (x, y) -> x)
                .reverse();
    }

    protected VectorTime vectorTimeZero() {
        return VectorTime$.MODULE$.Zero();
    }

    public static final class NoStackTraceException extends Exception {
        public NoStackTraceException(final String message) {
            super(message);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }
}
