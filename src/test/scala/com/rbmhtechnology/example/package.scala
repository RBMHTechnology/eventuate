/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology

import com.rbmhtechnology.eventuate.Versioned

package object example {
  def printOrder(versions: Seq[Versioned[Order]]): Unit = {
    if (versions.size > 1) {
      println("Conflict:")
      versions.zipWithIndex.foreach {
        case (version, idx) => println(s"- version ${idx}: ${version.value}")
      }
    } else {
      versions.headOption.map(_.value).foreach(println)
    }
  }
}
