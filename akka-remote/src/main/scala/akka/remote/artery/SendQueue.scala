/**
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.remote.artery

import java.util.Queue

import akka.stream.stage.GraphStage
import akka.stream.stage.OutHandler
import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.stage.GraphStageLogic
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue
import akka.stream.stage.GraphStageWithMaterializedValue
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueueTail
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.concurrent.Promise
import scala.util.Try
import scala.util.Success
import scala.util.Failure

import akka.actor.ActorSelectionMessage
import akka.event.Logging
import akka.stream.Attributes.LogLevels
import akka.stream.stage.StageLogging

/**
 * INTERNAL API
 */
private[remote] object SendQueue {
  trait ProducerApi[T] {
    def offer(message: T): Boolean

    def isEnabled: Boolean
  }

  trait QueueValue[T] extends ProducerApi[T] {
    def inject(queue: Queue[T]): Unit
  }

  private trait WakeupSignal {
    def wakeup(): Unit
  }
}

/**
 * INTERNAL API
 */
private[remote] final class SendQueue[T] extends GraphStageWithMaterializedValue[SourceShape[T], SendQueue.QueueValue[T]] {
  import SendQueue._

  val out: Outlet[T] = Outlet("SendQueue.out")
  override val shape: SourceShape[T] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, QueueValue[T]) = {
    @volatile var needWakeup = false
    val queuePromise = Promise[Queue[T]]()

    val debug = inheritedAttributes.get[LogLevels].map(_.onElement == Logging.DebugLevel).getOrElse(false)

    val logic = new GraphStageLogic(shape) with OutHandler with WakeupSignal with StageLogging {

      // using a local field for the consumer side of queue to avoid volatile access
      private var consumerQueue: Queue[T] = null

      private val wakeupCallback = getAsyncCallback[Unit] { _ ⇒
        if (isAvailable(out))
          tryPush()
      }

      override def preStart(): Unit = {
        implicit val ec = materializer.executionContext
        queuePromise.future.onComplete(getAsyncCallback[Try[Queue[T]]] {
          case Success(q) ⇒
            consumerQueue = q
            needWakeup = true
            if (isAvailable(out))
              tryPush()
          case Failure(e) ⇒
            failStage(e)
        }.invoke)
      }

      override def onPull(): Unit = {
        if (consumerQueue ne null)
          tryPush()
      }

      @tailrec private def tryPush(firstAttempt: Boolean = true): Unit = {
        consumerQueue.poll() match {
          case null ⇒
            needWakeup = true
            // additional poll() to grab any elements that might missed the needWakeup
            // and have been enqueued just after it
            if (firstAttempt)
              tryPush(firstAttempt = false)
          case elem ⇒
            if (debug) {
              val msgClass = elem match {
                case env: OutboundEnvelope ⇒ env.message match {
                  case ActorSelectionMessage(m, _, _) ⇒ m.getClass
                  case m                              ⇒ m.getClass
                }
                case _ ⇒ elem.getClass
              }
              log.debug("heartbeat-debug: SendQueue [{}] push, msg class [{}]", msgClass.getName)
            }
            needWakeup = false // there will be another onPull
            push(out, elem)
        }
      }

      // external call
      override def wakeup(): Unit = {
        wakeupCallback.invoke(())
      }

      override def postStop(): Unit = {
        // TODO quarantine will currently always be done when control stream is terminated, see issue #21359
        if (consumerQueue ne null)
          consumerQueue.clear()
        super.postStop()
      }

      setHandler(out, this)
    }

    val queueValue = new QueueValue[T] {
      @volatile private var producerQueue: Queue[T] = null

      override def inject(q: Queue[T]): Unit = {
        producerQueue = q
        queuePromise.success(q)
      }

      override def offer(message: T): Boolean = {
        val q = producerQueue
        if (q eq null) throw new IllegalStateException("offer not allowed before injecting the queue")
        val result = q.offer(message)
        if (result && needWakeup) {
          needWakeup = false
          logic.wakeup()
        }
        result
      }

      override def isEnabled: Boolean = true
    }

    (logic, queueValue)

  }
}
