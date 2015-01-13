/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology

import scala.util.control.NoStackTrace

package object eventuate {
  val boom = new Exception("boom") with NoStackTrace
}
