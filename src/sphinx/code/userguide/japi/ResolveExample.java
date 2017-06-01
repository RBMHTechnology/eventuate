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

//#automated-conflict-resolution

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import com.rbmhtechnology.eventuate.*;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

import static userguide.japi.DocUtils.append;

//#

public class ResolveExample {

  public static class Auto {

    //#automated-conflict-resolution

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
                final Stream<Versioned<Collection<String>>> conflictingVersions = versionedState.getAll().stream()
                    .sorted((v1, v2) -> {
                      if (v1.systemTimestamp() == v2.systemTimestamp()) {
                        return v1.creator().compareTo(v2.creator());
                      }
                      return v1.systemTimestamp() > v2.systemTimestamp() ? -1 : 1;
                    });

                final VectorTime winnerTimestamp = conflictingVersions.findFirst().get().vectorTimestamp();
                versionedState = versionedState.resolve(winnerTimestamp);
              }
            })
            .build();
      }
    }
    //#
  }

  public static class Interactive {

    //#interactive-conflict-resolution

    class ExampleActor extends AbstractEventsourcedActor {

      private ConcurrentVersions<Collection<String>, String> versionedState =
        ConcurrentVersionsTree.create(Collections.emptyList(), (s, a) -> append(s, a));

      public ExampleActor(String id, ActorRef eventLog) {
        super(id, eventLog);
      }

      @Override
      public AbstractActor.Receive createOnCommand() {
        return receiveBuilder()
            .match(Append.class, cmd -> versionedState.conflict(),
                cmd -> getSender().tell(new AppendRejected(cmd.entry, versionedState.getAll()), getSelf())
            )
            .match(Append.class, cmd -> {
              // ....
            })
            .match(Resolve.class, cmd -> persist(new Resolved(cmd.selectedTimestamp), ResultHandler.on(
                evt -> { /* reply to sender omitted ... */ },
                err -> { /* reply to sender omitted ... */ }
            )))
            .build();
      }

      @Override
      public AbstractActor.Receive createOnEvent() {
        return receiveBuilder()
            .match(Appended.class, evt ->
                versionedState = versionedState.update(evt.entry, getLastVectorTimestamp(), getLastSystemTimestamp(), getLastEmitterId())
            )
            .match(Resolved.class, evt ->
                versionedState = versionedState.resolve(evt.selectedTimestamp, getLastVectorTimestamp(), getLastSystemTimestamp())
            )
            .build();
      }
    }

    // Command
    class Append {
      public final String entry;

      public Append(String entry) {
        this.entry = entry;
      }
    }

    // Command reply
    class AppendRejected {
      public final String entry;
      public final Collection<Versioned<Collection<String>>> conflictingVersions;

      public AppendRejected(String entry, Collection<Versioned<Collection<String>>> conflictingVersions) {
        this.entry = entry;
        this.conflictingVersions = conflictingVersions;
      }
    }

    // Command
    class Resolve {
      public final VectorTime selectedTimestamp;

      public Resolve(VectorTime selectedTimestamp) {
        this.selectedTimestamp = selectedTimestamp;
      }
    }

    // Command reply
    class Resolved {
      public final VectorTime selectedTimestamp;

      public Resolved(VectorTime selectedTimestamp) {
        this.selectedTimestamp = selectedTimestamp;
      }
    }
  }
  //#
}
