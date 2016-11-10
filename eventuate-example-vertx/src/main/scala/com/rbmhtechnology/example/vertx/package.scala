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

package com.rbmhtechnology.example

import io.vertx.core.{ AsyncResult, DeploymentOptions, Handler, Vertx }

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.reflect.ClassTag

package object vertx {

  object ExampleVertxExtensions {

    implicit class PromiseHandler[A](promise: Promise[A]) {
      def asVertxHandler: Handler[AsyncResult[A]] = new Handler[AsyncResult[A]] {
        override def handle(res: AsyncResult[A]): Unit = {
          if (res.succeeded()) {
            promise.success(res.result())
          } else {
            promise.failure(res.cause())
          }
        }
      }
    }

    implicit class RichVertxDeployment(vertx: Vertx) {
      def deploy[T](options: DeploymentOptions = new DeploymentOptions())(implicit t: ClassTag[T], ec: ExecutionContext): Future[String] = {
        val promise = Promise[String]
        vertx.deployVerticle(t.runtimeClass.getName, options, promise.asVertxHandler)
        promise.future
      }
    }
  }
}
