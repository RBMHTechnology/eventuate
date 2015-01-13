/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
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
