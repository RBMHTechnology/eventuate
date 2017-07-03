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

package com.rbmhtechnology.example.japi.querydb;

import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.rbmhtechnology.eventuate.AbstractEventsourcedWriter;
import com.rbmhtechnology.example.japi.querydb.Emitter.AddressUpdated;
import com.rbmhtechnology.example.japi.querydb.Emitter.CustomerCreated;
import javaslang.collection.Vector;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

public class Writer extends AbstractEventsourcedWriter<Long, Void> {

    private final Session session;
    private PreparedStatement insertCustomerStmt;
    private PreparedStatement updateCustomerStmt;
    private PreparedStatement updateProgressStmt;

    private Vector<BoundStatement> batch = Vector.empty();

    public Writer(final String id, final ActorRef eventLog, final Session session) {
        super(id, eventLog);
        this.session = session;

        insertCustomerStmt = session.prepare("INSERT INTO CUSTOMER (id, first, last, address) VALUES (?, ?, ?, ?)");
        updateCustomerStmt = session.prepare("UPDATE CUSTOMER SET address = ? WHERE id = ?");
        updateProgressStmt = session.prepare("UPDATE PROGRESS SET sequence_nr = ? WHERE id = 0");

        setOnEvent(ReceiveBuilder
                .match(CustomerCreated.class, c -> batch = batch.append(insertCustomerStmt.bind(c.cid, c.first, c.last, c.address)))
                .match(AddressUpdated.class, u -> batch = batch.append(updateCustomerStmt.bind(u.address, u.cid)))
                .build());
    }

    @Override
    public int replayBatchSize() {
        return 16;
    }

    @Override
    public CompletionStage<Void> onWrite() {
        final Long snr = lastSequenceNr();
        final CompletableFuture<Void> res = sequence(batch.map(session::executeAsync).map(this::toFuture))
                .thenCompose(rs -> toFuture(session.executeAsync(updateProgressStmt.bind(snr))))
                .thenApply(rs -> null);
        batch = Vector.empty();
        return res;
    }

    @Override
    public CompletionStage<Long> onRead() {
        return toFuture(session.executeAsync("SELECT sequence_nr FROM PROGRESS WHERE id = 0"))
                .thenApply(rs -> rs.isExhausted() ? 0L : rs.one().getLong(0));
    }

    @Override
    public Optional<Long> onReadSuccess(final Long result) {
        return Optional.of(result + 1L);
    }

    private <T> CompletableFuture<Void> sequence(final Iterable<CompletableFuture<T>> futures) {
        return CompletableFuture.allOf(Iterables.toArray(futures, CompletableFuture.class));
    }

    private <T> CompletableFuture<T> toFuture(final ListenableFuture<T> listenableFuture) {
        return toCompletableFuture(listenableFuture, context().dispatcher());
    }

    private <T> CompletableFuture<T> toCompletableFuture(final ListenableFuture<T> listenableFuture, final Executor executor) {
        final CompletableFuture<T> ft = new CompletableFuture<>();
        listenableFuture.addListener(() -> {
            try {
                ft.complete(listenableFuture.get());
            } catch (Throwable e) {
                ft.completeExceptionally(e);
            }
        }, executor);

        return ft;
    }
}
