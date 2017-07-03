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

package com.rbmhtechnology.example.japi.ordermgnt;

import javaslang.collection.List;

/**
 * An immutable order.
 */
public class Order {

    private final String id;
    private final List<String> items;
    private final boolean cancelled;

    public Order(final String id) {
        this(id, List.empty(), false);
    }

    Order(final String id, final List<String> items, final boolean cancelled) {
        this.id = id;
        this.items = items;
        this.cancelled = cancelled;
    }

    public String getId() {
        return id;
    }

    public List<String> getItems() {
        return items.reverse();
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public Order addItem(String item) {
        return new Order(id, items.prepend(item), cancelled);
    }

    public Order removeItem(String item) {
        return new Order(id, items.filter(i -> !i.equals(item)), cancelled);
    }

    public Order cancel() {
        return new Order(id, items, true);
    }

    public String toString() {
        return String.format("[%s] items=%s cancelled=%s", id, getItems().toString(), cancelled);
    }
}
