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
import akka.japi.pf.ReceiveBuilder;
import com.rbmhtechnology.eventuate.AbstractEventsourcedView;
import com.rbmhtechnology.example.japi.ordermgnt.OrderActor.OrderEvent;
import javaslang.collection.HashMap;
import javaslang.collection.Map;

public class OrderView extends AbstractEventsourcedView {
    private Map<String, Integer> updateCounts;

    public OrderView(String replicaId, ActorRef eventLog) {
        super(String.format("j-ov-%s", replicaId), eventLog);
        this.updateCounts = HashMap.empty();

        setOnCommand(ReceiveBuilder
                .match(GetUpdateCount.class, this::handleGetUpdateCount)
                .build());

        setOnEvent(ReceiveBuilder
                .match(OrderEvent.class, this::handleOrderEvent)
                .build());
    }

    public void handleGetUpdateCount(final GetUpdateCount cmd) {
        final String orderId = cmd.orderId;
        sender().tell(new GetUpdateCountSuccess(orderId, updateCounts.get(orderId).getOrElse(0)), self());
    }

    public void handleOrderEvent(final OrderEvent evt) {
        final String id = evt.orderId;
        updateCounts = updateCounts.put(id, updateCounts.get(id).map(cnt -> cnt + 1).getOrElse(1));
    }

    // ------------------------------------------------------------------------------
    //  Domain commands
    // ------------------------------------------------------------------------------

    public static class GetUpdateCount extends OrderId {
        public GetUpdateCount(String orderId) {
            super(orderId);
        }
    }

    public static class GetUpdateCountSuccess extends OrderId {
        public final int count;

        public GetUpdateCountSuccess(String orderId, int count) {
            super(orderId);
            this.count = count;
        }
    }
}
