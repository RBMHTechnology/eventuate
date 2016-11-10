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

package com.rbmhtechnology.example.vertx.japi;

import com.rbmhtechnology.eventuate.adapter.vertx.japi.rx.StorageProvider;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.buffer.Buffer;
import rx.Observable;

import java.io.File;

public class DiskStorageProvider implements StorageProvider {
  private final Vertx vertx;
  private final String path;

  public DiskStorageProvider(String path, Vertx vertx) {
    this.vertx = vertx;
    this.path = path;

    new File(path).mkdirs();
  }

  @Override
  public Observable<Long> readProgress(String id) {
    return vertx.fileSystem().readFileObservable(path(id))
      .map(v -> Long.valueOf(v.toString()))
      .onErrorReturn(err -> 0L);
  }

  @Override
  public Observable<Long> writeProgress(String id, Long sequenceNr) {
    return vertx.fileSystem().writeFileObservable(path(id), Buffer.buffer(sequenceNr.toString()))
      .map(x -> sequenceNr);
  }

  private String path(final String logName) {
    return String.format("%s/progress-%s.txt", path, logName);
  }
}
