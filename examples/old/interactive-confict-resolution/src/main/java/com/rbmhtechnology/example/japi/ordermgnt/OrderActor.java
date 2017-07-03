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
import com.rbmhtechnology.eventuate.AbstractEventsourcedActor;
import com.rbmhtechnology.eventuate.ConcurrentVersions;
import com.rbmhtechnology.eventuate.ConcurrentVersionsTree;
import com.rbmhtechnology.eventuate.ResultHandler;
import com.rbmhtechnology.eventuate.SnapshotMetadata;
import com.rbmhtechnology.eventuate.Versioned;
import com.rbmhtechnology.eventuate.VersionedAggregate;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.rbmhtechnology.eventuate.VersionedAggregate.AggregateDoesNotExistException;
import static com.rbmhtechnology.eventuate.VersionedAggregate.DomainCmd;
import static com.rbmhtechnology.eventuate.VersionedAggregate.DomainEvt;
import static com.rbmhtechnology.eventuate.VersionedAggregate.Resolve;
import static com.rbmhtechnology.eventuate.VersionedAggregate.Resolved;

public class OrderActor extends AbstractEventsourcedActor {
    private final String orderId;
    private String replicaId;

    private VersionedAggregate<Order, OrderCommand, OrderEvent> order;

    private BiFunction<Order, OrderCommand, OrderEvent> commandValidation = (o, c) -> {
        if (c instanceof CreateOrder)
            return ((CreateOrder) c).event.withCreator(replicaId);
        else
            return c.event;
    };

    private BiFunction<Order, OrderEvent, Order> eventProjection = (o, e) -> {
        if (e instanceof OrderCreated)
            return new Order(((OrderCreated) e).orderId);
        else if (e instanceof OrderCancelled)
            return o.cancel();
        else if (e instanceof OrderItemAdded)
            return o.addItem(((OrderItemAdded) e).item);
        else if (e instanceof OrderItemRemoved)
            return o.removeItem(((OrderItemRemoved) e).item);
        else throw new IllegalArgumentException("event not supported: " + e);
    };

    public OrderActor(String orderId, String replicaId, ActorRef eventLog) {
        super(String.format("j-%s-%s", orderId, replicaId), eventLog);
        this.orderId = orderId;
        this.replicaId = replicaId;
        this.order = VersionedAggregate.create(orderId, commandValidation, eventProjection, OrderDomainCmd.instance, OrderDomainEvt.instance);

        setOnCommand(ReceiveBuilder
                .match(CreateOrder.class, c -> order.validateCreate(c, processCommand(orderId, sender(), self())))
                .match(OrderCommand.class, c -> order.validateUpdate(c, processCommand(orderId, sender(), self())))
                .match(Resolve.class, c -> order.validateResolve(c.selected(), replicaId, processCommand(orderId, sender(), self())))
                .match(GetState.class, c -> sender().tell(createStateFromAggregate(orderId, order), self()))
                .match(SaveSnapshot.class, c -> saveState(sender(), self()))
                .build());

        setOnEvent(ReceiveBuilder
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

        setOnSnapshot(ReceiveBuilder
                .match(ConcurrentVersionsTree.class, s -> {
                    order = order.withAggregate(((ConcurrentVersionsTree<Order, OrderEvent>) s).withProjection(eventProjection));
                    System.out.println(String.format("[%s] Snapshot loaded:", orderId));
                    printOrder(order.getVersions());
                })
                .build());

        setOnRecover(ResultHandler
                .onSuccess(v -> {
                    System.out.println(String.format("[%s] Recovery complete:", orderId));
                    printOrder(order.getVersions());
                }));
    }

    @Override
    public Optional<String> getAggregateId() {
        return Optional.of(orderId);
    }

    private <E> ResultHandler<E> processCommand(final String orderId, final ActorRef sender, final ActorRef self) {
        return ResultHandler.on(
                evt -> processEvent(evt, sender, self),
                err -> sender.tell(new CommandFailure(orderId, err), self)
        );
    }

    private <E> void processEvent(final E event, final ActorRef sender, final ActorRef self) {
        persist(event, ResultHandler.on(
                evt -> sender.tell(new CommandSuccess(orderId), self),
                err -> sender.tell(new CommandFailure(orderId, err), self)
        ));
    }

    private void saveState(final ActorRef sender, final ActorRef self) {
        if (order.getAggregate().isPresent()) {
            save(order.getAggregate().get(), ResultHandler.on(
                    metadata -> sender.tell(new SaveSnapshotSuccess(orderId, metadata), self),
                    err -> sender.tell(new SaveSnapshotFailure(orderId, err), self)
            ));
        } else {
            sender.tell(new SaveSnapshotFailure(orderId, new AggregateDoesNotExistException(orderId)), self);
        }
    }

    private GetStateSuccess createStateFromAggregate(final String id, final VersionedAggregate<Order, OrderCommand, OrderEvent> agg) {
        return new GetStateSuccess(agg.getAggregate().map(ConcurrentVersions::getAll).map(getVersionMap(id)).orElseGet(Collections::emptyMap));
    }

    private Function<List<Versioned<Order>>, Map<String, List<Versioned<Order>>>> getVersionMap(final String id) {
        return versions -> {
            HashMap<String, List<Versioned<Order>>> map = new HashMap<>();
            map.put(id, versions);
            return map;
        };
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
                return ((OrderCreated) evt).creator;
            } else {
                return "";
            }
        }
    }

    // ------------------------------------------------------------------------------
    //  Order commands
    // ------------------------------------------------------------------------------

    public static abstract class OrderCommand<T extends OrderEvent> extends OrderId {
        public final T event;

        protected OrderCommand(final String orderId, final T event) {
            super(orderId);
            this.event = event;
        }
    }

    public static class CreateOrder extends OrderCommand<OrderCreated> {
        public CreateOrder(String orderId) {
            super(orderId, new OrderCreated(orderId));
        }
    }

    public static class CancelOrder extends OrderCommand<OrderCancelled> {
        public CancelOrder(String orderId) {
            super(orderId, new OrderCancelled(orderId));
        }
    }

    public abstract static class ModifyOrderItems<T extends OrderEvent> extends OrderCommand<T> {
        public final String item;

        protected ModifyOrderItems(final String orderId, final T event, final String item) {
            super(orderId, event);
            this.item = item;
        }
    }

    public static class AddOrderItem extends ModifyOrderItems<OrderItemAdded> {
        public AddOrderItem(final String orderId, final String item) {
            super(orderId, new OrderItemAdded(orderId, item), item);
        }
    }

    public static class RemoveOrderItem extends ModifyOrderItems<OrderItemRemoved> {
        public RemoveOrderItem(final String orderId, final String item) {
            super(orderId, new OrderItemRemoved(orderId, item), item);
        }
    }

    // ------------------------------------------------------------------------------
    //  Order events
    // ------------------------------------------------------------------------------

    public static abstract class OrderEvent extends OrderId {
        protected OrderEvent(String orderId) {
            super(orderId);
        }
    }

    public static class OrderCreated extends OrderEvent {
        public final String creator;

        public OrderCreated(final String orderId) {
            this(orderId, "");
        }

        public OrderCreated(final String orderId, final String creator) {
            super(orderId);
            this.creator = creator;
        }

        public OrderCreated withCreator(final String creator) {
            return new OrderCreated(this.orderId, creator);
        }
    }

    public static class OrderCancelled extends OrderEvent {
        public OrderCancelled(String orderId) {
            super(orderId);
        }
    }

    public abstract static class OrderItemModified extends OrderEvent {
        public final String item;

        protected OrderItemModified(final String orderId, final String item) {
            super(orderId);
            this.item = item;
        }
    }

    public static class OrderItemAdded extends OrderItemModified {
        public OrderItemAdded(final String orderId, final String item) {
            super(orderId, item);
        }
    }

    public static class OrderItemRemoved extends OrderItemModified {
        public OrderItemRemoved(final String orderId, final String item) {
            super(orderId, item);
        }
    }

    // ------------------------------------------------------------------------------
    // Order queries + replies
    // ------------------------------------------------------------------------------

    public static class GetState {
        public static final GetState instance = new GetState();

        private GetState() {
        }
    }

    public static class GetStateSuccess {
        public final Map<String, List<Versioned<Order>>> state;

        private GetStateSuccess(final Map<String, List<Versioned<Order>>> state) {
            this.state = Collections.unmodifiableMap(state);
        }

        public static GetStateSuccess empty() {
            return new GetStateSuccess(Collections.emptyMap());
        }

        public GetStateSuccess merge(final GetStateSuccess that) {
            final Map<String, List<Versioned<Order>>> result = new HashMap<>();
            result.putAll(this.state);
            result.putAll(that.state);
            return new GetStateSuccess(result);
        }
    }

    public static class GetStateFailure {
        public final Throwable cause;

        public GetStateFailure(Throwable cause) {
            this.cause = cause;
        }
    }

    // ------------------------------------------------------------------------------
    // General replies
    // ------------------------------------------------------------------------------

    public static class CommandSuccess extends OrderId {
        public CommandSuccess(String orderId) {
            super(orderId);
        }
    }

    public static class CommandFailure extends OrderId {
        public final Throwable cause;

        public CommandFailure(final String orderId, final Throwable cause) {
            super(orderId);
            this.cause = cause;
        }
    }

    // ------------------------------------------------------------------------------
    // Snapshot command and replies
    // ------------------------------------------------------------------------------

    public static class SaveSnapshot extends OrderId {
        public SaveSnapshot(String orderId) {
            super(orderId);
        }
    }

    public static class SaveSnapshotSuccess extends OrderId {
        public final SnapshotMetadata metadata;

        public SaveSnapshotSuccess(final String orderId, final SnapshotMetadata metadata) {
            super(orderId);
            this.metadata = metadata;
        }
    }

    public static class SaveSnapshotFailure extends OrderId {
        public final Throwable cause;

        public SaveSnapshotFailure(final String orderId, final Throwable cause) {
            super(orderId);
            this.cause = cause;
        }
    }
}
