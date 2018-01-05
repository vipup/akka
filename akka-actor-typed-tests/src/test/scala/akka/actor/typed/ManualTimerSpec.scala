package akka.actor.typed

import scala.concurrent.duration._

import akka.actor.typed.scaladsl.Actor
import akka.testkit.typed.TestKit
import akka.testkit.typed.scaladsl.{ ManualTime, TestProbe }
import org.scalatest.WordSpecLike

class ManualTimerSpec extends TestKit() with ManualTime with WordSpecLike {
  //#manual-scheduling-simple
  "A timer" must {
    "schedule non-repeated ticks" in {
      case object Tick
      case object Tock

      val probe = TestProbe[Tock.type]()
      val behv = Actor.withTimers[Tick.type] { timer ⇒
        timer.startSingleTimer("T", Tick, 10.millis)
        Actor.immutable { (ctx, Tick) ⇒
          probe.ref ! Tock
          Actor.same
        }
      }

      val ref = spawn(behv)

      scheduler.expectNoMessageFor(9.millis, probe)

      scheduler.timePasses(2.millis)
      probe.expectMsg(Tock)

      scheduler.expectNoMessageFor(10.seconds, probe)
    }
  }
  //#manual-scheduling-simple
}
