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
import akka.actor.Status;
import akka.japi.Creator;
import akka.pattern.Patterns;
import akka.testkit.TestProbe;
import javaslang.control.Try;
import org.junit.Before;
import org.junit.Test;
import scala.reflect.ClassTag$;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import static scala.compat.java8.FutureConverters.toJava;

public class AbstractEventsourcedWriterSpec extends BaseSpec {

    private static final int REPLAY_BATCH_SIZE = 2;
    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    public static class TestEventsourcedWriter extends AbstractEventsourcedWriter<String, String> {
        private final ActorRef appProbe;
        private final ActorRef rwProbe;

        public TestEventsourcedWriter(final String id, final ActorRef logProbe, final ActorRef appProbe, final ActorRef rwProbe) {
            super(id, logProbe);
            this.appProbe = appProbe;
            this.rwProbe = rwProbe;
        }

        @Override
        public int replayBatchSize() {
            return REPLAY_BATCH_SIZE;
        }

        @Override
        public CompletionStage<String> onRead() {
            return ask(rwProbe, "r", String.class, TIMEOUT);
        }

        @Override
        public CompletionStage<String> onWrite() {
            return ask(rwProbe, "w", String.class, TIMEOUT);
        }

        @Override
        public Optional<Long> onReadSuccess(final String result) {
            appProbe.tell(result, self());
            return super.onReadSuccess(result);
        }

        @Override
        public void onWriteSuccess(final String result) {
            appProbe.tell(result, self());
            super.onWriteSuccess(result);
        }

        @Override
        public void onReadFailure(final Throwable cause) {
            appProbe.tell(cause, self());
            super.onReadFailure(cause);
        }

        @Override
        public void onWriteFailure(final Throwable cause) {
            appProbe.tell(cause, self());
            super.onWriteFailure(cause);
        }

        private <T> CompletionStage<T> ask(final ActorRef actor, final Object payload, final Class<T> clazz, final Duration timeout) {
            return toJava(Patterns.ask(actor, payload, timeout.toMillis()).mapTo(ClassTag$.MODULE$.apply(clazz)));
        }
    }

    private Integer instanceId;
    private TestProbe logProbe;
    private TestProbe appProbe;
    private TestProbe rwProbe;

    private DurableEvent eventA = createEvent("a", 1L);
    private DurableEvent eventB = createEvent("b", 2L);
    private DurableEvent eventC = createEvent("c", 2L);

    @Before
    public void beforeEach() {
        instanceId = getInstanceId();
        logProbe = new TestProbe(system);
        appProbe = new TestProbe(system);
        rwProbe = new TestProbe(system);
    }

    private ActorRef unrecoveredEventWriter() {
        return system.actorOf(Props.create(TestEventsourcedWriter.class,
                (Creator<TestEventsourcedWriter>) () -> new TestEventsourcedWriter(EMITTER_ID, logProbe.ref(), appProbe.ref(), rwProbe.ref())));
    }

    private void processRead(final Try<String> result) {
        rwProbe.expectMsg("r");
        processResult(result);
    }

    private void processWrite(final Try<String> result) {
        rwProbe.expectMsg("w");
        processResult(result);
    }

    private void processResult(final Try<String> result) {
        result.onSuccess(s -> rwProbe.sender().tell(new Status.Success(s), rwProbe.ref()))
                .onFailure(e -> rwProbe.sender().tell(new Status.Failure(e), rwProbe.ref()));
    }

    @Test
    public void shouldProcessInitialReadOnStartup() {
        unrecoveredEventWriter();

        processRead(Try.success("rs"));
        appProbe.expectMsg("rs");
    }

    @Test
    public void shouldWriteEventsOnRecover() {
        final ActorRef actor = unrecoveredEventWriter();

        processRead(Try.success("rs"));
        appProbe.expectMsg("rs");

        processRecover(actor, EMITTER_ID, instanceId, logProbe, REPLAY_BATCH_SIZE, eventA, eventB, eventC);
        processWrite(Try.success("wr"));
        appProbe.expectMsg("wr");
    }

    @Test
    public void shouldReportFailureOnFailedRead() {
        unrecoveredEventWriter();

        processRead(Try.failure(FAILURE));
        appProbe.expectMsg(FAILURE);
    }

    @Test
    public void shouldReportFailureOnFailedWrite() {
        final ActorRef actor = unrecoveredEventWriter();

        processRead(Try.success("rs"));
        appProbe.expectMsg("rs");

        processRecover(actor, EMITTER_ID, instanceId, logProbe, REPLAY_BATCH_SIZE, eventA, eventB, eventC);
        processWrite(Try.failure(FAILURE));
        appProbe.expectMsg(FAILURE);
    }
}
