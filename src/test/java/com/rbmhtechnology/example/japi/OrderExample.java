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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.*;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;

import com.rbmhtechnology.eventuate.ReplicationConnection;
import com.rbmhtechnology.eventuate.ReplicationEndpoint;
import com.rbmhtechnology.eventuate.VersionedAggregate.*;
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog;
import com.typesafe.config.ConfigFactory;

import static com.rbmhtechnology.example.japi.OrderActor.*;
import static com.rbmhtechnology.example.japi.OrderView.*;

public class OrderExample extends AbstractActor {
    private static Pattern pExit    = Pattern.compile("^exit\\s*");
    private static Pattern pState   = Pattern.compile("^state\\s*");
    private static Pattern pCount   = Pattern.compile("^count\\s+(\\w+)\\s*");
    private static Pattern pCreate  = Pattern.compile("^create\\s+(\\w+)\\s*");
    private static Pattern pCancel  = Pattern.compile("^cancel\\s+(\\w+)\\s*");
    private static Pattern pSave  =   Pattern.compile("^save\\s+(\\w+)\\s*");
    private static Pattern pAdd     = Pattern.compile("^add\\s+(\\w+)\\s+(\\w+)\\s*");
    private static Pattern pRemove  = Pattern.compile("^remove\\s+(\\w+)\\s+(\\w+)\\s*");
    private static Pattern pResolve = Pattern.compile("^resolve\\s+(\\w+)\\s+(\\d+)\\s*");

    private ActorRef manager;
    private ActorRef view;

    private BufferedReader reader;

    public OrderExample(ActorRef manager, ActorRef view) {
        this.manager = manager;
        this.view = view;

        this.reader = new BufferedReader(new InputStreamReader(System.in));

        receive(ReceiveBuilder
                .match(GetStateSuccess.class, r -> {
                    r.getState().values().stream().forEach(OrderActor::printOrder);
                    prompt();
                })
                .match(GetStateFailure.class, r -> {
                    System.out.println(r.getCause().getMessage());
                    prompt();
                })
                .match(SaveSnapshotSuccess.class, r -> {
                    System.out.println(String.format("[%s] saved snapshot at sequence number %d", r.getOrderId(), r.getMetadata().sequenceNr()));
                    prompt();
                })
                .match(SaveSnapshotFailure.class, r -> {
                    System.out.println(String.format("[%s] saved snapshot failed: %s", r.getOrderId(), r.getCause()));
                    prompt();
                })
                .match(GetUpdateCountSuccess.class, r -> {
                    System.out.println("[" + r.getOrderId() + "]" + " update count = " + r.getCount());
                    prompt();
                })
                .match(CommandFailure.class, r -> r.getCause() instanceof ConflictDetectedException, r -> {
                    ConflictDetectedException cause = (ConflictDetectedException) r.getCause();
                    System.out.println(cause.getMessage() + ", select one of the following versions to resolve conflict");
                    OrderActor.printOrder(cause.getVersions());
                    prompt();
                })
                .match(CommandFailure.class, r -> {
                    System.out.println(r.getCause().getMessage());
                    prompt();
                })
                .match(CommandSuccess.class, r -> prompt())
                .match(String.class, cmd -> process(cmd)).build());
    }

    private void prompt() throws IOException {
        String line = reader.readLine();
        if (line != null)
            self().tell(line, null);
    }

    private void process(String cmd) throws IOException {
        // okay, a bit eager, but anyway ...
        Matcher mExit    = pExit.matcher(cmd);
        Matcher mState   = pState.matcher(cmd);
        Matcher mCount   = pCount.matcher(cmd);
        Matcher mCreate  = pCreate.matcher(cmd);
        Matcher mCancel  = pCancel.matcher(cmd);
        Matcher mSave    = pSave.matcher(cmd);
        Matcher mAdd     = pAdd.matcher(cmd);
        Matcher mRemove  = pRemove.matcher(cmd);
        Matcher mResolve = pResolve.matcher(cmd);

        if (mExit.matches()) {
            getContext().system().terminate();
        } else if (mState.matches()) {
            manager.tell(GetState.instance, self());
        } else if (mCount.matches()) {
            view.tell(new GetUpdateCount(mCount.group(1)), self());
        } else if (mCreate.matches()) {
            manager.tell(new CreateOrder(mCreate.group(1)), self());
        } else if (mCancel.matches()) {
            manager.tell(new CancelOrder(mCancel.group(1)), self());
        } else if (mSave.matches()) {
            manager.tell(new SaveSnapshot(mSave.group(1)), self());
        } else if (mAdd.matches()) {
            manager.tell(new AddOrderItem(mAdd.group(1), mAdd.group(2)), self());
        } else if (mRemove.matches()) {
            manager.tell(new RemoveOrderItem(mRemove.group(1), mRemove.group(2)), self());
        } else if (mResolve.matches()) {
            manager.tell(new Resolve(mResolve.group(1), Integer.parseInt(mResolve.group(2)), ""), self());
        } else {
            System.out.println("unknown command: " + cmd);
            prompt();
        }
    }

    @Override
    public void preStart() throws Exception {
        prompt();
    }

    @Override
    public void postStop() throws Exception {
        reader.close();
    }

    public static void main(String[] args) {
        if (args[1].equals("recover")) {
            throw new UnsupportedOperationException("Java version of example application doesn't support disaster recovery yet");
        }

        ActorSystem system = ActorSystem.create(ReplicationConnection.DefaultRemoteSystemName(), ConfigFactory.load(args[0]));
        ReplicationEndpoint endpoint = ReplicationEndpoint.create(id -> LeveldbEventLog.props(id, "j", true), system);

        ActorRef manager = system.actorOf(Props.create(OrderManager.class, endpoint.id(), endpoint.logs().apply(ReplicationEndpoint.DefaultLogName())));
        ActorRef view = system.actorOf(Props.create(OrderView.class, endpoint.id(), endpoint.logs().apply(ReplicationEndpoint.DefaultLogName())));
        ActorRef driver = system.actorOf(Props.create(OrderExample.class, manager, view));

        endpoint.activate();
    }
}
