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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;

import com.rbmhtechnology.eventuate.AbstractEventsourcedActor;
import com.rbmhtechnology.eventuate.ConcurrentVersions;
import com.rbmhtechnology.eventuate.Versioned;
import com.rbmhtechnology.eventuate.VersionedAggregate;

import static com.rbmhtechnology.eventuate.VersionedAggregate.*;

public class OrderActor extends AbstractEventsourcedActor {
    private String orderId;
    private VersionedAggregate<Order, OrderCommand, OrderEvent> order;

    private BiFunction<Order, OrderCommand, OrderEvent> commandValidation = (o, c) -> {
        if (c instanceof CreateOrder)
            return ((CreateOrder)c).createEvent().withCreator(replicaId());
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

    public OrderActor(String orderId, String replicaId, ActorRef eventLog) {
        super(replicaId, eventLog);
        this.orderId = orderId;
        this.order = VersionedAggregate.create(
                orderId,
                commandValidation,
                eventProjection,
                OrderDomainCmd.instance,
                OrderDomainEvt.instance);

        onReceiveCommand(ReceiveBuilder
                .match(CreateOrder.class, c -> processCommand(c.getOrderId(), () -> order.doValidateCreate(c)))
                .match(OrderCommand.class, c -> processCommand(c.getOrderId(), () -> order.doValidateUpdate(c)))
                .match(Resolve.class, c -> processCommand(c.id(), () -> order.doValidateResolve(c.selected(), replicaId())))
                .match(GetState.class, c -> sender().tell(new GetStateSuccess(ordersVersions()), self())).build());

        onReceiveEvent(ReceiveBuilder
                .match(OrderCreated.class, e -> {
                    order = order.handleCreated(e, lastVectorTimestamp(), lastSequenceNr());
                    if (!recovering()) printOrder(order.getVersions());
                })
                .match(OrderEvent.class, e -> {
                    order = order.handleUpdated(e, lastVectorTimestamp(), lastSequenceNr());
                    if (!recovering()) printOrder(order.getVersions());
                })
                .match(Resolved.class, e -> {
                    order = order.handleResolved(e, lastVectorTimestamp(), lastSequenceNr());
                    if (!recovering()) printOrder(order.getVersions());
                })
                .build());
    }

    @Override
    public Optional<String> getAggregateId() {
        return Optional.of(orderId);
    }

    @Override
    public void recovered() {
        printOrder(order.getVersions());
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

    private Map<String, List<Versioned<Order>>> ordersVersions() {
        return order.getAggregate().map(ConcurrentVersions::getAll).map(this::orderVersions).orElse(new HashMap<>());
    }

    private Map <String, List<Versioned<Order>>> orderVersions(List<Versioned<Order>> versions) {
        HashMap<String, List<Versioned<Order>>> map = new HashMap<>();
        map.put(orderId, versions);
        return map;
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

    public static class OrderDomainCmd implements DomainCmd<OrderCommand> {
        public static OrderDomainCmd instance = new OrderDomainCmd();

        public String origin(OrderCommand cmd) {
            return "";
        }
    }

    public static class OrderDomainEvt implements DomainEvt<OrderEvent> {
        public static OrderDomainEvt instance = new OrderDomainEvt();

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

        public GetStateSuccess merge(GetStateSuccess that) {
            Map<String, List<Versioned<Order>>> result = new HashMap();
            result.putAll(this.state);
            result.putAll(that.state);
            return new GetStateSuccess(result);
        }
    }

    public static class GetStateFailure {
        private Throwable cause;

        public GetStateFailure(Throwable cause) {
            this.cause = cause;
        }

        public Throwable getCause() {
            return cause;
        }
    }
}
