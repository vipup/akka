/**
 * Copyright (C) 2017 Lightbend Inc. <http://www.lightbend.com/>
 */
package akka.actor.typed;

//#manual-scheduling-simple
import java.util.concurrent.TimeUnit;
import static com.typesafe.config.ConfigFactory.parseString;

import akka.testkit.typed.ExplicitlyTriggeredScheduler;
import org.junit.Test;

import scala.concurrent.duration.Duration;

import akka.actor.typed.javadsl.Actor;
import akka.testkit.typed.TestKit;
import akka.testkit.typed.javadsl.TestProbe;

public class ManualTimerTest extends TestKit {
  ExplicitlyTriggeredScheduler scheduler;

  public ManualTimerTest() {
    super(parseString("akka.scheduler.implementation = \"akka.testkit.typed.ExplicitlyTriggeredScheduler\""));
    this.scheduler = (ExplicitlyTriggeredScheduler) system().scheduler();
  }

  static final class Tick {}
  static final class Tock {}

  @Test
  public void testScheduleNonRepeatedTicks() {
    TestProbe<Tock> probe = new TestProbe<>(system(), testkitSettings());
    Behavior<Tick> behavior = Actor.withTimers(timer -> {
      timer.startSingleTimer("T", new Tick(), Duration.create(10, TimeUnit.MILLISECONDS));
      return Actor.immutable( (ctx, tick) -> {
        probe.ref().tell(new Tock());
        return Actor.same();
      });
    });

    spawn(behavior);

    scheduler.expectNoMessageFor(Duration.create(9, TimeUnit.MILLISECONDS), probe);

    scheduler.timePasses(Duration.create(2, TimeUnit.MILLISECONDS));
    probe.expectMsgType(Tock.class);

    scheduler.expectNoMessageFor(Duration.create(10, TimeUnit.SECONDS), probe);
  }


}
//#manual-scheduling-simple