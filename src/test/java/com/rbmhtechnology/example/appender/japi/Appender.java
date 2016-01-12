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

package com.rbmhtechnology.example.appender.japi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Optional;

import akka.actor.*;
import akka.japi.pf.ReceiveBuilder;

import com.rbmhtechnology.eventuate.AbstractEventsourcedActor;
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog;

public class Appender {

    static class Append implements Serializable {
        private String entry;

        public Append(String entry) {
            this.entry = entry;
        }

        public String getEntry() {
            return entry;
        }
    }

    static class Appended implements Serializable {
        private String entry;

        public Appended(String entry) {
            this.entry = entry;
        }

        public String getEntry() {
            return entry;
        }
    }

    static class AppenderActor extends AbstractEventsourcedActor {
        private String groupId;
        private ArrayList<String> currentState = new ArrayList<>();

        public AppenderActor(String id, String groupId, ActorRef eventLog) {
            super(id, eventLog);
            this.groupId = groupId;
            onReceiveCommand(ReceiveBuilder
                    .match(Append.class, this::handleAppend)
                    .build());
            onReceiveEvent(ReceiveBuilder
                    .match(Appended.class, this::handleAppended)
                    .build());
        }

        public Optional<String> getAggregateId() {
            return Optional.of(groupId);
        }

        private void handleAppend(Append append) {
            persist(new Appended(append.getEntry()), (evt, err) -> {
                if (err == null) {
                    sender().tell("success", self());
                } else {
                    sender().tell("failure", self());
                }
            });
        }

        private void handleAppended(Appended appended) {
            currentState.add(appended.getEntry());
            System.out.println(String.format("[%s] appended: %s", id(), appended.getEntry()));
            System.out.println(String.format("[%s] state:    %s", id(), currentState));
        }
    }

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("example");
        ActorRef log = system.actorOf(LeveldbEventLog.props("appender-log", "", true));
        ActorRef appender1 = system.actorOf(Props.create(AppenderActor.class, "appender1", "group", log));
        ActorRef appender2 = system.actorOf(Props.create(AppenderActor.class, "appender2", "group", log));

        appender1.tell(new Append("a"), null);
        appender2.tell(new Append("b"), null);
    }
}
