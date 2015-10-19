/*
 * Copyright (C) 2015 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

package com.rbmhtechnology.example.japi;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import scala.concurrent.Future;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.dispatch.OnComplete;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.Patterns;

import com.rbmhtechnology.eventuate.AbstractEventsourcedView;

import static java.util.stream.Collectors.reducing;

import static com.rbmhtechnology.eventuate.VersionedAggregate.*;
import static com.rbmhtechnology.example.japi.OrderActor.*;

public class OrderManager extends AbstractEventsourcedView {

    private final String replicaId;
    private final Map<String, ActorRef> orderActors;


    public OrderManager(String replicaId, ActorRef eventLog) {
        super(String.format("j-om-%s", replicaId), eventLog);
        this.replicaId = replicaId;
        this.orderActors = new HashMap<>();

        onReceiveCommand(ReceiveBuilder
                .match(OrderCommand.class, c -> orderActor(c.getOrderId()).tell(c, sender()))
                .match(SaveSnapshot.class, c -> orderActor(c.getOrderId()).tell(c, sender()))
                .match(Resolve.class, c -> orderActor(c.id()).tell(c, sender()))
                .match(GetState.class, c -> orderActors.isEmpty(), c -> replyStateZero(sender()))
                .match(GetState.class, c -> !orderActors.isEmpty(), c -> replyState(sender())).build());

        onReceiveEvent(ReceiveBuilder
                .match(OrderCreated.class, e -> !orderActors.containsKey(e.getOrderId()), e -> orderActor(e.getOrderId())).build());
    }

    private ActorRef orderActor(String orderId) {
        if (!orderActors.containsKey(orderId)) {
            ActorRef orderActor = context().actorOf(Props.create(OrderActor.class, orderId, replicaId, eventLog()));
            orderActors.put(orderId, orderActor);
        }
        return orderActors.get(orderId);
    }

    private void replyStateZero(ActorRef target) {
        target.tell(new GetStateSuccess(new HashMap<>()), self());
    }

    private void replyState(ActorRef target) {
        OnComplete<GetStateSuccess> completionHandler = new OnComplete<GetStateSuccess>() {
            public void onComplete(Throwable failure, GetStateSuccess success) throws Throwable {
                if (failure == null) {
                    target.tell(success, self());
                }  else {
                    target.tell(new GetStateFailure(failure), self());
                }
            }
        };

        Stream<Future<GetStateSuccess>> resultStream = orderActors.values().stream()
                .map(this::asyncGetState);

        Futures.sequence(resultStream::iterator, context().dispatcher())
                .map(resultsReducer, context().dispatcher())
                .onComplete(completionHandler, context().dispatcher());
    }

    private Future<GetStateSuccess> asyncGetState(ActorRef actor) {
        return Patterns.ask(actor, GetState.instance, 10000L).map(resultMapper, context().dispatcher());
    }

    private static Mapper<Object, GetStateSuccess> resultMapper = new Mapper<Object, GetStateSuccess>() {
        public GetStateSuccess apply(Object result) {
            return (GetStateSuccess)result;
        }
    };

    private static Mapper<Iterable<GetStateSuccess>, GetStateSuccess> resultsReducer = new Mapper<Iterable<GetStateSuccess>, GetStateSuccess>() {
        public GetStateSuccess apply(Iterable<GetStateSuccess> results) {
            return StreamSupport.stream(results.spliterator(), false).collect(reducing((a, b) -> a.merge(b))).get();
        }
    };
}
