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

import static japi.DocUtils.append;

//#detecting-concurrent-update
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import com.rbmhtechnology.eventuate.AbstractEventsourcedActor;
import com.rbmhtechnology.eventuate.VectorTime;

import java.util.Collection;
import java.util.Collections;

//#
public class ConcurrentExample {

  //#detecting-concurrent-update

  class ExampleActor extends AbstractEventsourcedActor {
    private Collection<String> currentState = Collections.emptyList();
    private VectorTime updateTimestamp = VectorTime.Zero();

    public ExampleActor(String id, ActorRef eventLog) {
      super(id, eventLog);

      setOnEvent(ReceiveBuilder
        .match(Messages.AppendedEvent.class, evt -> {
          if (updateTimestamp.lt(lastVectorTimestamp())) {
            // regular update
            currentState = append(currentState, evt.entry);
            updateTimestamp = lastVectorTimestamp();
          } else if (updateTimestamp.conc(lastVectorTimestamp())) {
            // concurrent update
            // TODO: track conflicting versions
          }
        })
        .build());
    }
  }
  //#

  // TODO Make this into an executable example
}
