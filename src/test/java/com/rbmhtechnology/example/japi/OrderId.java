/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.example.japi;

import java.io.Serializable;

public abstract class OrderId implements Serializable {
    private String orderId;

    protected OrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOrderId() {
        return orderId;
    }
}
