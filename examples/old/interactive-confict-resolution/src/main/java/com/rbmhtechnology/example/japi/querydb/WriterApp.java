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
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog;
import com.rbmhtechnology.example.japi.querydb.Emitter.CreateCustomer;
import com.rbmhtechnology.example.japi.querydb.Emitter.UpdateAddress;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;
import java.util.Random;
import java.util.function.Consumer;

import static akka.actor.ActorRef.noSender;
import static scala.compat.java8.JFunction.proc;

public class WriterApp {

    // ---------------------------------------------------------------
    //  Assumption: Cassandra 2.1 or higher running on localhost:9042
    // ---------------------------------------------------------------

    public static void main(final String[] args) {
        withQueryDB(false, session -> {
            final ActorSystem system = ActorSystem.create("example-querydb", ConfigFactory.load(args[0]));
            final ActorRef log = system.actorOf(LeveldbEventLog.props("example", "log", true));

            final ActorRef emitter = system.actorOf(Props.create(Emitter.class, () -> new Emitter("emitter", log)));
            final ActorRef writer = system.actorOf(Props.create(Writer.class, () -> new Writer("writer", log, session)));

            emitter.tell(new CreateCustomer("Martin", "Krasser", "Somewhere 1"), noSender());
            Patterns.ask(emitter, new CreateCustomer("Volker", "Stampa", "Somewhere 2"), Duration.ofSeconds(5).toMillis())
                    .onComplete(proc(result -> {
                        if (result.isSuccess()) {
                            emitter.tell(new UpdateAddress(((Emitter.CustomerCreated) result.get()).cid,
                                    String.format("Somewhere %d", new Random().nextInt(10))), noSender());
                        } else {
                            result.failed().get().printStackTrace();
                        }
                    }), system.dispatcher());

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            system.terminate();
        });
    }

    private static void withQueryDB(final Boolean drop, final Consumer<Session> f) {
        final Session session = createQueryDB(drop);
        try {
            f.accept(session);
        } finally {
            dropQueryDB(session);
        }
    }

    private static Session createQueryDB(final Boolean drop) {
        final Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
        final Session session = cluster.connect();

        if (drop) {
            session.execute("DROP KEYSPACE IF EXISTS QUERYDB");
        }

        session.execute("CREATE KEYSPACE IF NOT EXISTS QUERYDB WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        session.execute("USE QUERYDB");

        session.execute("CREATE TABLE IF NOT EXISTS CUSTOMER (id bigint, first text, last text, address text, PRIMARY KEY (id))");
        session.execute("CREATE TABLE IF NOT EXISTS PROGRESS (id bigint, sequence_nr bigint, PRIMARY KEY (id))");
        session.execute("INSERT INTO PROGRESS (id, sequence_nr) VALUES(0, 0) IF NOT EXISTS");

        return session;
    }

    private static void dropQueryDB(final Session session) {
        session.close();
        session.getCluster().close();
    }
}
