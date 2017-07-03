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
import com.rbmhtechnology.eventuate.AbstractEventsourcedActor;
import com.rbmhtechnology.eventuate.ResultHandler;

import java.io.Serializable;

public class Emitter extends AbstractEventsourcedActor {

    private Long highestCustomerId = 0L;

    public Emitter(String id, ActorRef eventLog) {
        super(id, eventLog);

        setOnCommand(ReceiveBuilder
                .match(CreateCustomer.class,
                        cmd -> persist(new CustomerCreated(highestCustomerId + 1, cmd.first, cmd.last, cmd.address), ResultHandler.on(
                                c -> sender().tell(c, self()),
                                this::handleFailure
                        )))
                .match(UpdateAddress.class, cmd -> cmd.cid <= highestCustomerId,
                        cmd -> persist(new AddressUpdated(cmd.cid, cmd.address), ResultHandler.on(
                                c -> sender().tell(c, self()),
                                this::handleFailure
                        )))
                .match(UpdateAddress.class,
                        cmd -> sender().tell(new Exception(String.format("Customer with %s does not exist", cmd.cid)), self())
                )
                .build());

        setOnEvent(ReceiveBuilder
                .match(CustomerCreated.class,
                        evt -> highestCustomerId = evt.cid
                )
                .build());
    }

    private void handleFailure(final Throwable failure) {
        throw new RuntimeException(failure);
    }

    // ---------------
    // Domain Commands
    // ---------------

    public static class CreateCustomer implements Serializable {
        public final String first;
        public final String last;
        public final String address;

        public CreateCustomer(final String first, final String last, final String address) {
            this.first = first;
            this.last = last;
            this.address = address;
        }
    }

    public static class UpdateAddress implements Serializable {
        public final Long cid;
        public final String address;

        public UpdateAddress(final Long cid, final String address) {
            this.cid = cid;
            this.address = address;
        }
    }

    // -------------
    // Domain Events
    // -------------

    public static class CustomerCreated implements Serializable {
        public final Long cid;
        public final String first;
        public final String last;
        public final String address;

        public CustomerCreated(final Long cid, final String first, final String last, final String address) {
            this.cid = cid;
            this.first = first;
            this.last = last;
            this.address = address;
        }
    }

    public static class AddressUpdated implements Serializable {
        public final Long cid;
        public final String address;

        public AddressUpdated(final Long cid, final String address) {
            this.cid = cid;
            this.address = address;
        }
    }
}
