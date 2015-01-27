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

import fj.data.List;

/**
 * An immutable order.
 */
public class Order {

    // ------------------------------------------------------------------------------------
    //  Order instances are shared across actors/threads and should therefore be immutable
    // ------------------------------------------------------------------------------------

    private String id;
    private List<String> items;
    private boolean cancelled;

    public Order(String id) {
        this(id, List.nil(), false);
    }

    private Order(String id, List<String> items, boolean cancelled) {
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
        return new Order(id, items.cons(item), cancelled);
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
