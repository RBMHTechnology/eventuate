package com.rbmhtechnology.eventuate.utilities

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

class RestarterActor(props: Props, name: Option[String]) extends Actor {

  import RestarterActor._

  var child: ActorRef = newActor
  var requester: ActorRef = _

  override def receive = {
    case Restart =>
      requester = sender()
      context.watch(child)
      context.stop(child)
    case Terminated(_) =>
      child = newActor
      requester ! child
    case msg =>
      child forward msg
  }

  private def newActor: ActorRef =
    name.map(context.actorOf(props, _)).getOrElse(context.actorOf(props))
}

object RestarterActor {
  case object Restart

  implicit val timeout = Timeout(timeoutDuration)

  def restartActor(restarterRef: ActorRef): ActorRef =
    (restarterRef ? Restart).mapTo[ActorRef].await

  def props(props: Props, name: Option[String] = None) = Props(new RestarterActor(props, name))
}
