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

package userguide.japi;

import static userguide.japi.DocUtils.append;

//#tracking-conflicting-versions

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import com.rbmhtechnology.eventuate.*;

import java.util.Collection;
import java.util.Collections;
//#

public class TrackingExample {

  //#tracking-conflicting-versions

  class ExampleActor extends AbstractEventsourcedActor {

    private ConcurrentVersions<Collection<String>, String> versionedState =
      ConcurrentVersionsTree.create(Collections.emptyList(), (s, a) -> append(s, a));

    public ExampleActor(String id, ActorRef eventLog) {
      super(id, eventLog);
    }

    @Override
    public AbstractActor.Receive createOnEvent() {
        return receiveBuilder()
            .match(Appended.class, evt -> {
                versionedState = versionedState.update(evt.entry, getLastVectorTimestamp(), getLastSystemTimestamp(), getLastEmitterId());

                if (versionedState.conflict()) {
                    final Collection<Versioned<Collection<String>>> all = versionedState.getAll();
                    // TODO: resolve conflicting versions
                } else {
                    final Collection<String> currentState = versionedState.getAll().get(0).value();
                    // ...
                }
            })
            .build();
      }
  }
  //#
}
