package akka.actor.typed

//#manual-scheduling-simple
import scala.concurrent.duration._

import akka.actor.typed.scaladsl.Actor

import org.scalatest.WordSpecLike
import akka.testkit.typed.TestKit
import akka.testkit.typed.scaladsl.{ ManualTime, TestProbe }

class ManualTimerSpec extends TestKit() with ManualTime with WordSpecLike {
  "A timer" must {
    "schedule non-repeated ticks" in {
      case object Tick
      case object Tock

      val probe = TestProbe[Tock.type]()
      val behavior = Actor.withTimers[Tick.type] { timer ⇒
        timer.startSingleTimer("T", Tick, 10.millis)
        Actor.immutable { (ctx, Tick) ⇒
          probe.ref ! Tock
          Actor.same
        }
      }

      spawn(behavior)

      scheduler.expectNoMessageFor(9.millis, probe)

      scheduler.timePasses(2.millis)
      probe.expectMsg(Tock)

      scheduler.expectNoMessageFor(10.seconds, probe)
    }
  }
}
//#manual-scheduling-simple
