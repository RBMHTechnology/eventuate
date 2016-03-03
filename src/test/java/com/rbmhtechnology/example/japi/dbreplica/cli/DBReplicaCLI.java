/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

package com.rbmhtechnology.example.japi.dbreplica.cli;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import com.rbmhtechnology.example.japi.dbreplica.cdc.AssetCdcOutbound;
import com.rbmhtechnology.example.japi.dbreplica.domain.AssetDoesNotExistException;
import com.rbmhtechnology.example.japi.dbreplica.event.AssetEvent.AssetContentUpdated;
import com.rbmhtechnology.example.japi.dbreplica.event.AssetEvent.AssetCreated;
import com.rbmhtechnology.example.japi.dbreplica.event.AssetEvent.AssetSubjectUpdated;
import com.rbmhtechnology.example.japi.dbreplica.service.AssetFinder;
import javaslang.Tuple2;
import javaslang.collection.Map;
import javaslang.control.Match;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DuplicateKeyException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static com.rbmhtechnology.example.japi.dbreplica.cli.Command.Parameters.*;

public class DBReplicaCLI extends AbstractActor {

    private final AssetCdcOutbound service;
    private final AssetFinder assetFinder;

    private BufferedReader reader;

    public DBReplicaCLI(final AssetCdcOutbound service, final AssetFinder assetFinder) {
        this.service = service;
        this.assetFinder = assetFinder;
        this.reader = new BufferedReader(new InputStreamReader(System.in));

        receive(ReceiveBuilder
                .match(String.class, this::handleCommand)
                .build()
        );
    }

    private void handleCommand(final String command) throws IOException {
        final Tuple2<Command, Map<String, String>> cmdWithValues = Command.fromString(command);
        final Command cmd = cmdWithValues._1;
        final Map<String, String> values = cmdWithValues._2;

        try {
            switch (cmd) {
                case CREATE:
                    service.handle(new AssetCreated(values.get(ID).get(), values.get(SUBJECT).get(), values.get(CONTENT).get()));
                    break;
                case SUBJECT:
                    service.handle(new AssetSubjectUpdated(values.get(ID).get(), values.get(SUBJECT).get()));
                    break;
                case CONTENT:
                    service.handle(new AssetContentUpdated(values.get(ID).get(), values.get(CONTENT).get()));
                    break;
                case LIST:
                    assetFinder.findAll().forEach(System.out::println);
                    break;
                case UNKNOWN:
                    // fall through
                default:
                    if (!StringUtils.isBlank(command)) {
                        System.out.println(String.format("unknown command: %s", command));
                    }
            }
        } catch (AssetDoesNotExistException e) {
            System.out.println(e.getMessage());
        } catch (DuplicateKeyException e) {
            System.out.println(String.format("asset with id '%s' is already present", values.get(ID).get()));
        }
        prompt();
    }

    private void prompt() throws IOException {
        final ActorRef self = self();
        final String line = reader.readLine();

        Match.of(line)
                .when((String l) -> l.equals("exit")).thenRun(() -> getContext().system().terminate())
                .otherwiseRun(l -> self.tell(l, null));
    }

    @Override
    public void preStart() throws Exception {
        prompt();
    }
}
