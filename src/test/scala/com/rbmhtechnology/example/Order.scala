/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.example

case class Order(id: String, items: List[String] = Nil, cancelled: Boolean = false) {
  def addItem(item: String): Order =
    copy(items = item :: items)

  def removeItem(item: String): Order =
    copy(items = items.filterNot(_ == item))

  def cancel: Order =
    copy(cancelled = true)

  override def toString() =
    s"[${id}] items=${items.reverse.mkString(",")} cancelled=${cancelled}"
}
