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

package com.rbmhtechnology.example.japi.ordermgnt;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.dispatch.Futures;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.Patterns;
import com.rbmhtechnology.eventuate.AbstractEventsourcedView;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Future;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.rbmhtechnology.eventuate.VersionedAggregate.Resolve;
import static com.rbmhtechnology.example.japi.ordermgnt.OrderActor.GetState;
import static com.rbmhtechnology.example.japi.ordermgnt.OrderActor.GetStateFailure;
import static com.rbmhtechnology.example.japi.ordermgnt.OrderActor.GetStateSuccess;
import static com.rbmhtechnology.example.japi.ordermgnt.OrderActor.OrderCommand;
import static com.rbmhtechnology.example.japi.ordermgnt.OrderActor.OrderCreated;
import static com.rbmhtechnology.example.japi.ordermgnt.OrderActor.SaveSnapshot;
import static scala.compat.java8.JFunction.func;
import static scala.compat.java8.JFunction.proc;

public class OrderManager extends AbstractEventsourcedView {

    private final String replicaId;
    private Map<String, ActorRef> orderActors;

    public OrderManager(String replicaId, ActorRef eventLog) {
        super(String.format("j-om-%s", replicaId), eventLog);
        this.replicaId = replicaId;
        this.orderActors = HashMap.empty();

        setOnCommand(ReceiveBuilder
                .match(OrderCommand.class, c -> orderActor(c.orderId).tell(c, sender()))
                .match(SaveSnapshot.class, c -> orderActor(c.orderId).tell(c, sender()))
                .match(Resolve.class, c -> orderActor(c.id()).tell(c, sender()))
                .match(GetState.class, c -> orderActors.isEmpty(), c -> replyStateZero(sender()))
                .match(GetState.class, c -> !orderActors.isEmpty(), c -> replyState(sender()))
                .build());

        setOnEvent(ReceiveBuilder
                .match(OrderCreated.class, e -> !orderActors.containsKey(e.orderId), e -> orderActor(e.orderId))
                .build());
    }

    private ActorRef orderActor(final String orderId) {
        return orderActors.get(orderId)
                .getOrElse(() -> {
                    final ActorRef orderActor = context().actorOf(Props.create(OrderActor.class, orderId, replicaId, eventLog()));
                    orderActors = orderActors.put(orderId, orderActor);
                    return orderActor;
                });
    }

    private void replyStateZero(ActorRef target) {
        target.tell(GetStateSuccess.empty(), self());
    }

    private void replyState(ActorRef target) {
        final ExecutionContextExecutor dispatcher = context().dispatcher();

        Futures.sequence(getActorStates()::iterator, dispatcher)
                .map(func(this::toStateSuccess), dispatcher)
                .onComplete(proc(result -> {
                    if (result.isSuccess()) {
                        target.tell(result.get(), self());
                    } else {
                        target.tell(new GetStateFailure(result.failed().get()), self());
                    }
                }), dispatcher);
    }

    private Stream<Future<GetStateSuccess>> getActorStates() {
        return orderActors.values().toJavaStream().map(this::asyncGetState);
    }

    private Future<GetStateSuccess> asyncGetState(final ActorRef actor) {
        return Patterns.ask(actor, GetState.instance, 10000L).map(func(o -> (GetStateSuccess) o), context().dispatcher());
    }

    private GetStateSuccess toStateSuccess(final Iterable<GetStateSuccess> states) {
        return StreamSupport.stream(states.spliterator(), false).reduce(GetStateSuccess::merge).get();
    }
}
