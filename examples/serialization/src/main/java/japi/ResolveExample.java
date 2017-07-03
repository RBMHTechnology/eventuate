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

//#automated-conflict-resolution

package japi;

import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import com.rbmhtechnology.eventuate.*;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

import static japi.DocUtils.append;

// TODO turn this into a runnable example

//#

public class ResolveExample {
  public static class Auto {
    //#automated-conflict-resolution
    class ExampleActor extends AbstractEventsourcedActor {

      private ConcurrentVersions<Collection<String>, String> versionedState =
        ConcurrentVersionsTree.create(Collections.emptyList(), DocUtils::append);

      public ExampleActor(String id, ActorRef eventLog) {
        super(id, eventLog);

        setOnEvent(ReceiveBuilder
          .match(Messages.AppendedEvent.class, evt -> {
            versionedState = versionedState.update(evt.entry, lastVectorTimestamp(), lastSystemTimestamp(), lastEmitterId());

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
          .build());
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

        setOnCommand(ReceiveBuilder
          .match(AppendCommand.class, cmd -> versionedState.conflict(),
            cmd -> sender().tell(new AppendRejectedCommandReply(cmd.entry, versionedState.getAll()), self())
          )
          .match(AppendCommand.class, cmd -> {
            // ....
          })
          .match(ResolveCommand.class, cmd -> persist(new ResolvedCommandReply(cmd.selectedTimestamp), ResultHandler.on(
            evt -> { /* reply to sender omitted ... */ },
            err -> { /* reply to sender omitted ... */ }
          )))
          .build());

        setOnEvent(ReceiveBuilder
          .match(Messages.AppendedEvent.class, evt ->
            versionedState = versionedState.update(evt.entry, lastVectorTimestamp(), lastSystemTimestamp(), lastEmitterId())
          )
          .match(ResolvedCommandReply.class, evt ->
            versionedState = versionedState.resolve(evt.selectedTimestamp, lastVectorTimestamp(), lastSystemTimestamp())
          )
          .build());
      }
    }

    class AppendCommand {
      public final String entry;

      public AppendCommand(String entry) {
        this.entry = entry;
      }
    }

    class AppendRejectedCommandReply {
      public final String entry;
      public final Collection<Versioned<Collection<String>>> conflictingVersions;

      public AppendRejectedCommandReply(String entry, Collection<Versioned<Collection<String>>> conflictingVersions) {
        this.entry = entry;
        this.conflictingVersions = conflictingVersions;
      }
    }

    class ResolveCommand {
      public final VectorTime selectedTimestamp;

      public ResolveCommand(VectorTime selectedTimestamp) {
        this.selectedTimestamp = selectedTimestamp;
      }
    }

    class ResolvedCommandReply {
      public final VectorTime selectedTimestamp;

      public ResolvedCommandReply(VectorTime selectedTimestamp) {
        this.selectedTimestamp = selectedTimestamp;
      }
    }
  }
  //#
}
