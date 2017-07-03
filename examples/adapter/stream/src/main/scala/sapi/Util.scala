package sapi

import akka.actor.ActorSystem

/**
  * @author mslinn */
object Util {
  // Pause for messages to be displayed before shutting down Akka
  def pauseThenStop(seconds: Int = 1)(implicit system: ActorSystem): Unit = {
    import system.dispatcher
    import scala.concurrent.duration._
    import scala.language.postfixOps
    system.scheduler.scheduleOnce(seconds seconds) {
      system.terminate()
      ()
    }
    ()
  }
}
