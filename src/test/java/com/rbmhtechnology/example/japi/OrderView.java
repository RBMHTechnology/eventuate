/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
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
