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
import akka.actor.Status;
import akka.japi.Creator;
import akka.testkit.TestProbe;
import com.rbmhtechnology.eventuate.EventsourcingProtocol.Written;
import com.rbmhtechnology.eventuate.ReplicationProtocol.GetReplicationProgress;
import com.rbmhtechnology.eventuate.ReplicationProtocol.GetReplicationProgressSuccess;
import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationWrite;
import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationWriteFailure;
import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationWriteSuccess;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

public class AbstractEventsourcedProcessorSpec extends BaseSpec {

    private static final int REPLAY_BATCH_SIZE = 2;
    private static final String EMPTY = "";

    public static class TestEventsourcedProcessor extends AbstractEventsourcedProcessor {

        private final ActorRef appProbe;

        public TestEventsourcedProcessor(final String id, final ActorRef srcProbe, final ActorRef targetProbe, final ActorRef appProbe) {
            super(id, srcProbe, targetProbe);
            this.appProbe = appProbe;

            setOnProcessEvent(ProcessBuilder
                    .match(String.class, s -> Arrays.asList(s + "-1", s + "-2"))
                    .build());
        }

        @Override
        public int replayBatchSize() {
            return REPLAY_BATCH_SIZE;
        }

        @Override
        public Optional<Long> onReadSuccess(final long result) {
            return super.onReadSuccess(result);
        }

        @Override
        public void onWriteSuccess(final long progress) {
            appProbe.tell(progress, self());
            super.onWriteSuccess(progress);
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
    }

    private Integer instanceId;
    private TestProbe srcProbe;
    private TestProbe targetProbe;
    private TestProbe appProbe;

    private DurableEvent eventA = createEvent("a", 1L, EMITTER_ID, EMPTY, timestamp(1L, 0));
    private DurableEvent eventA1 = createEvent("a-1", 0L, EMITTER_ID, EMPTY, timestamp(1L, 0));
    private DurableEvent eventA2 = createEvent("a-2", 0L, EMITTER_ID, EMPTY, timestamp(1L, 0));

    private DurableEvent eventB = createEvent("b", 2L, EMITTER_ID, EMPTY, timestamp(0, 1L));
    private DurableEvent eventB1 = createEvent("b-1", 0L, EMITTER_ID, EMPTY, timestamp(0, 1L));
    private DurableEvent eventB2 = createEvent("b-2", 0L, EMITTER_ID, EMPTY, timestamp(0, 1L));

    @Before
    public void beforeEach() {
        instanceId = getInstanceId();
        srcProbe = new TestProbe(system);
        targetProbe = new TestProbe(system);
        appProbe = new TestProbe(system);
    }

    private ActorRef unrecoveredStatelessEventProcessor() {
        return system.actorOf(Props.create(TestEventsourcedProcessor.class,
                (Creator<TestEventsourcedProcessor>) () -> new TestEventsourcedProcessor(EMITTER_ID, srcProbe.ref(), targetProbe.ref(), appProbe.ref())));
    }

    private ActorRef recoveredStatelessEventProcessor() {
        final ActorRef actor = unrecoveredStatelessEventProcessor();
        processReadSuccess(0);
        processReplay(actor, 1, instanceId, srcProbe, REPLAY_BATCH_SIZE);
        return actor;
    }

    private void processReadSuccess(final long progress) {
        targetProbe.expectMsg(new GetReplicationProgress(EMITTER_ID));
        processResult(new GetReplicationProgressSuccess(EMITTER_ID, progress, vectorTimeZero()));
    }

    private void processReadFailure() {
        targetProbe.expectMsg(new GetReplicationProgress(EMITTER_ID));
        processResult(new ReplicationProtocol.GetReplicationProgressFailure(FAILURE));
    }

    private void processWriteSuccess(final long progress, final DurableEvent... events) {
        targetProbe.expectMsg(new ReplicationWrite(toSeq(events), progress, EMITTER_ID, vectorTimeZero(), false, null));
        processResult(new ReplicationWriteSuccess(toSeq(events), progress, EMITTER_ID, vectorTimeZero(), false));
    }

    private void processWriteFailure(final long progress, final DurableEvent... events) {
        targetProbe.expectMsg(new ReplicationWrite(toSeq(events), progress, EMITTER_ID, vectorTimeZero(), false, null));
        processResult(new ReplicationWriteFailure(FAILURE));
    }

    private void processResult(final Object result) {
        targetProbe.sender().tell(new Status.Success(result), targetProbe.ref());
    }

    @Test
    public void shouldRecover() {
        recoveredStatelessEventProcessor();
    }

    @Test
    public void shouldProcessEvents() {
        final ActorRef actor = recoveredStatelessEventProcessor();

        actor.tell(new Written(eventA), getRef());
        actor.tell(new Written(eventB), getRef());

        processWriteSuccess(1, eventA1, eventA2);
        appProbe.expectMsg(1);

        processWriteSuccess(2, eventB1, eventB2);
        appProbe.expectMsg(2);
    }

    @Test
    public void shouldResumeOnFailedRead() {
        final ActorRef actor = unrecoveredStatelessEventProcessor();
        processReadFailure();

        appProbe.expectMsg(FAILURE);

        processReadSuccess(0);
        processReplay(actor, 1, instanceId + 1, srcProbe, REPLAY_BATCH_SIZE);
    }

    @Test
    public void shouldResumeOnFailedWrite() {
        final ActorRef actor = recoveredStatelessEventProcessor();

        actor.tell(new Written(eventA), getRef());
        processWriteFailure(1, eventA1, eventA2);

        appProbe.expectMsg(FAILURE);

        processReadSuccess(0);
        processReplay(actor, 1, instanceId + 1, srcProbe, REPLAY_BATCH_SIZE);
    }
}
