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

package japi;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

// TODO turn this into a runnable example
public class CrdtExample {
  //#or-set-service
  /** Java API of a replicated [[ORSet]] CRDT service. */
  class ORSetService<A> extends CRDTService<ORSet<A>, Set<A>> {

   /** @param serviceId Unique id of this service.
     * @param log Event log.
     * @param system Actor system.
     * @tparam A [[ORSet]] entry type. */
    ORSetService(String serviceId, ActorRef log, ActorSystem system) {
      super(serviceId, log, system);

      start();
    }

    public CompletionStage<Set<A>> add(String id, A entry) {
      return op(id, new AddOp(entry));
    }

    public CompletionStage<Set<A>> remove(String id, A entry) {
      return op(id, new RemoveOp(entry));
    }
  }
  //#

  abstract class CRDTService<A, B> {
    private final String serviceId;
    private final ActorRef log;
    private final ActorSystem system;

    protected CRDTService(String serviceId, ActorRef log, ActorSystem system) {
      this.serviceId = serviceId;
      this.log = log;
      this.system = system;
    }

    public CompletionStage<B> op(String id, Object op) {
      return CompletableFuture.completedFuture(null);
    }

    public void start() {}
  }

  interface ORSet<A> {}

  private class AddOp  {
    final Object entry;

    private AddOp(Object entry) {
      this.entry = entry;
    }
  }

  private class RemoveOp {
    final Object entry;

    private RemoveOp(Object entry) {
      this.entry = entry;
    }
  }
}
