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

import java.io.Serializable;
import java.util.Objects;

public class Event implements Serializable {
  private static final long serialVersionUID = 3151977002206184102L;

  private final String id;

  private Event(String id) {
    this.id = id;
  }

  public static Event create(String id) {
    return new Event(id);
  }

  public String id() {
    return id;
  }

  public Event copy(final String id) {
    return new Event(id);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Event event = (Event) o;
    return Objects.equals(id, event.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + id + ")";
  }
}
