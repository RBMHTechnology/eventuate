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

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;

import com.rbmhtechnology.eventuate.AbstractEventsourcedActor;
import com.rbmhtechnology.eventuate.Versioned;
import com.rbmhtechnology.eventuate.VersionedObjects;

import static com.rbmhtechnology.eventuate.VersionedObjects.*;
import static java.util.stream.Collectors.toMap;

public class OrderManager extends AbstractEventsourcedActor {
    private VersionedObjects<Order, OrderCommand, OrderEvent> orders;

    private BiFunction<Order, OrderCommand, OrderEvent> commandValidation = (o, c) -> {
        if (c instanceof CreateOrder)
            return ((CreateOrder)c).createEvent().withCreator(processId());
        else
            return c.createEvent();
    };

    private BiFunction<Order, OrderEvent, Order> eventProjection = (o, e) -> {
        if (e instanceof OrderCreated)
            return new Order(e.getOrderId());
        else if (e instanceof OrderCancelled)
            return o.cancel();
        else if (e instanceof OrderItemAdded)
            return o.addItem(((OrderItemAdded) e).getItem());
        else if (e instanceof OrderItemRemoved)
            return o.removeItem(((OrderItemRemoved) e).getItem());
        else throw new IllegalArgumentException("event not supported: " + e);
    };

    public OrderManager(String processId, ActorRef log) {
        super(processId, log);
        this.orders = VersionedObjects.create(
                commandValidation,
                eventProjection,
                OrderDomainCmd.instance,
                OrderDomainEvt.instance);

        onReceiveCommand(ReceiveBuilder
                .match(CreateOrder.class, c -> processCommand(c.getOrderId(), () -> orders.doValidateCreate(c)))
                .match(OrderCommand.class, c -> processCommand(c.getOrderId(), () -> orders.doValidateUpdate(c)))
                .match(Resolve.class, c -> processCommand(c.id(), () -> orders.doValidateResolve(c.withOrigin(processId()))))
                .match(GetState.class, c -> sender().tell(new GetStateSuccess(ordersSnapshot()), self())).build());

        onReceiveEvent(ReceiveBuilder
                .match(OrderCreated.class, e -> {
                    orders.handleCreated(e, lastVectorTimestamp(), lastSequenceNr());
                    if (!recovering()) printOrder(orders.getVersions(e.getOrderId()));
                })
                .match(OrderEvent.class, e -> {
                    orders.handleUpdated(e, lastVectorTimestamp(), lastSequenceNr());
                    if (!recovering()) printOrder(orders.getVersions(e.getOrderId()));
                })
                .match(Resolved.class, e -> {
                    orders.handleResolved(e, lastVectorTimestamp(), lastSequenceNr());
                    if (!recovering()) printOrder(orders.getVersions(e.id()));
                })
                .build());
    }

    private <E> void processCommand(String orderId, Supplier<E> cmdValidation) {
        try {
            processEvent(orderId, cmdValidation.get());
        } catch (Throwable err) {
            sender().tell(new CommandFailure(orderId, err), self());
        }
    }

    private <E> void processEvent(String orderId, E event) {
        persist(event, (evt, err) -> {
            if (err == null) {
                onEvent().apply(evt);
                sender().tell(new CommandSuccess(orderId), self());
            } else {
                sender().tell(new CommandFailure(orderId, err), self());
            }
        });
    }

    private Map<String, List<Versioned<Order>>> ordersSnapshot() {
        return orders.getCurrent().entrySet().stream().collect(toMap(
                entry -> entry.getKey(),
                entry -> entry.getValue().getAll()));
    }

    static void printOrder(List<Versioned<Order>> versions) {
        if (versions.size() > 1) {
            System.out.println("Conflict:");
            IntStream.range(0, versions.size()).forEach(i -> System.out.println("- version " + i + ": " + versions.get(i).value()));
        } else if (versions.size() == 1) {
            System.out.println(versions.get(0).value());
        }
    }

    // ------------------------------------------------------------------------------
    //  Type class instances needed by VersionedState
    // ------------------------------------------------------------------------------

    public static class OrderDomainCmd implements VersionedObjects.DomainCmd<OrderCommand> {
        public static OrderDomainCmd instance = new OrderDomainCmd();

        public String id(OrderCommand cmd) {
            return cmd.getOrderId();
        }

        public String origin(OrderCommand cmd) {
            return "";
        }
    }

    public static class OrderDomainEvt implements VersionedObjects.DomainEvt<OrderEvent> {
        public static OrderDomainEvt instance = new OrderDomainEvt();

        public String id(OrderEvent evt) {
            return evt.getOrderId();
        }

        public String origin(OrderEvent evt) {
            if (evt instanceof OrderCreated) {
                return ((OrderCreated) evt).getCreator();
            } else {
                return "";
            }
        }
    }

    // ------------------------------------------------------------------------------
    //  Domain commands
    // ------------------------------------------------------------------------------

    public static abstract class OrderCommand extends OrderId {
        protected OrderCommand(String orderId) {
            super(orderId);
        }

        abstract OrderEvent createEvent();
    }

    public static class CreateOrder extends OrderCommand {
        public CreateOrder(String orderId) {
            super(orderId);
        }

        public OrderCreated createEvent() {
            return new OrderCreated(getOrderId());
        }
    }

    public static class CancelOrder extends OrderCommand {
        public CancelOrder(String orderId) {
            super(orderId);
        }

        public OrderEvent createEvent() {
            return new OrderCancelled(getOrderId());
        }
    }

    public abstract static class ModifyOrderItems extends OrderCommand {
        private String item;

        protected ModifyOrderItems(String orderId, String item) {
            super(orderId);
            this.item = item;
        }

        public String getItem() {
            return item;
        }
    }

    public static class AddOrderItem extends ModifyOrderItems {
        public AddOrderItem(String orderId, String item) {
            super(orderId, item);
        }

        public OrderEvent createEvent() {
            return new OrderItemAdded(getOrderId(), getItem());
        }
    }

    public static class RemoveOrderItem extends ModifyOrderItems {
        public RemoveOrderItem(String orderId, String item) {
            super(orderId, item);
        }

        public OrderEvent createEvent() {
            return new OrderItemRemoved(getOrderId(), getItem());
        }
    }

    // ------------------------------------------------------------------------------
    //  Domain events
    // ------------------------------------------------------------------------------

    public static abstract class OrderEvent extends OrderId {
        protected OrderEvent(String orderId) {
            super(orderId);
        }
    }

    public static class OrderCreated extends OrderEvent {
        private String creator;

        public OrderCreated(String orderId) {
            this(orderId, "");
        }

        public OrderCreated(String orderId, String creator) {
            super(orderId);
            this.creator = creator;
        }

        public String getCreator() {
            return creator;
        }

        public OrderCreated withCreator(String creator) {
            return new OrderCreated(getOrderId(), creator);
        }
    }

    public static class OrderCancelled extends OrderEvent {
        public OrderCancelled(String orderId) {
            super(orderId);
        }
    }

    public abstract static class OrderItemModified extends OrderEvent {
        private String item;

        protected OrderItemModified(String orderId, String item) {
            super(orderId);
            this.item = item;
        }

        public String getItem() {
            return item;
        }
    }

    public static class OrderItemAdded extends OrderItemModified {
        public OrderItemAdded(String orderId, String item) {
            super(orderId, item);
        }
    }

    public static class OrderItemRemoved extends OrderItemModified {
        public OrderItemRemoved(String orderId, String item) {
            super(orderId, item);
        }
    }

    // ------------------------------------------------------------------------------
    //  Command replies
    // ------------------------------------------------------------------------------

    public static class CommandSuccess extends OrderId {
        public CommandSuccess(String orderId) {
            super(orderId);
        }
    }

    public static class CommandFailure extends OrderId {
        private Throwable cause;

        public CommandFailure(String orderId, Throwable cause) {
            super(orderId);
            this.cause = cause;
        }

        public Throwable getCause() {
            return cause;
        }
    }

    // ------------------------------------------------------------------------------
    //  Other commands
    // ------------------------------------------------------------------------------

    public static class GetState {
        private GetState() {}

        public static GetState instance = new GetState();
    }

    public static class GetStateSuccess {
        private Map<String, List<Versioned<Order>>> state;

        public GetStateSuccess(Map<String, List<Versioned<Order>>> state) {
            this.state = state;
        }

        public Map<String, List<Versioned<Order>>> getState() {
            return state;
        }
    }
}
