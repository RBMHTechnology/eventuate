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

package com.rbmhtechnology.eventuate.adapter

import com.rbmhtechnology.eventuate.EventsourcedView
import io.vertx.core.{ Future => VertxFuture, _ }
import io.vertx.rxjava.core.{ Vertx => RxVertx }
import rx.functions.Func1

import scala.concurrent.Promise
import scala.util.{ Failure, Success }

package object vertx {

  object VertxConverters {

    import scala.language.implicitConversions

    implicit def rxVertxToVertx(rxVertx: RxVertx): Vertx =
      rxVertx.getDelegate.asInstanceOf[Vertx]

    implicit def vertxToRxVertx(vertx: Vertx): RxVertx =
      new RxVertx(vertx)
  }

  object VertxHandlerConverters {

    implicit class Fn0AsHandler(fn: => Unit) {
      def asVertxHandler: Handler[Void] = new Handler[Void] {
        override def handle(event: Void): Unit = fn
      }
    }

    implicit class Fn1AsHandler[A](fn: A => Unit) {
      def asVertxHandler: Handler[A] = new Handler[A] {
        override def handle(event: A): Unit = fn(event)
      }
    }

    implicit class EventuateHandlerAsVertxHandler[A](h: EventsourcedView.Handler[A]) {
      def asVertxHandler: Handler[AsyncResult[A]] = new Handler[AsyncResult[A]] {
        override def handle(ar: AsyncResult[A]): Unit = {
          if (ar.succeeded()) {
            h(Success(ar.result()))
          } else {
            h(Failure(ar.cause()))
          }
        }
      }
    }

    implicit class HandlerAsEventuateHandler[A](h: Handler[AsyncResult[A]]) {
      def asEventuateHandler: EventsourcedView.Handler[A] = {
        case Success(res) => h.handle(VertxFuture.succeededFuture(res))
        case Failure(err) => h.handle(VertxFuture.failedFuture(err))
      }
    }

    implicit class PromiseAsVertxHandler[A](promise: Promise[A]) {
      def asVertxHandler: Handler[AsyncResult[A]] = new Handler[AsyncResult[A]] {
        override def handle(ar: AsyncResult[A]): Unit = {
          if (ar.succeeded()) {
            promise.success(ar.result())
          } else {
            promise.failure(ar.cause())
          }
        }
      }
    }
  }

  object RxConverters {

    implicit class Fn1AsRxFunc1[A, B](fn: A => B) {
      def asRx: Func1[A, B] = new Func1[A, B] {
        override def call(a: A): B = fn(a)
      }
    }
  }
}
