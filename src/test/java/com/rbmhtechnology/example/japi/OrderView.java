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

import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;

import com.rbmhtechnology.eventuate.AbstractEventsourcedView;
import com.rbmhtechnology.example.japi.OrderManager.*;

public class OrderView extends AbstractEventsourcedView {
    private Map<String, Integer> updateCounts;

    public OrderView(String id, ActorRef log) {
        super(id, log);
        this.updateCounts = new HashMap<String, Integer>();

        onReceiveCommand(ReceiveBuilder.match(GetUpdateCount.class, this::handleGetUpdateCount).build());
        onReceiveEvent(ReceiveBuilder.match(OrderEvent.class, this::handleOrderEvent).build());
    }

    public void handleGetUpdateCount(GetUpdateCount cmd) {
        String id = cmd.getOrderId();
        int count = updateCounts.getOrDefault(id, 0);
        sender().tell(new GetUpdateCountSuccess(id, count), self());
    }

    public void handleOrderEvent(OrderEvent evt) {
        String id = evt.getOrderId();
        Integer count = updateCounts.get(id);

        if (count == null) {
            updateCounts.put(id, 1);
        } else {
            updateCounts.put(id, count + 1);
        }
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
        private int count;

        public GetUpdateCountSuccess(String orderId, int count) {
            super(orderId);
            this.count = count;
        }

        public int getCount() {
            return count;
        }
    }
}
