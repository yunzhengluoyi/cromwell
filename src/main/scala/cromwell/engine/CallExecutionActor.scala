package cromwell.engine

import akka.actor.{Actor, Props}
import akka.event.{Logging, LoggingReceive}
import com.google.api.client.util.ExponentialBackOff
import cromwell.engine.backend._
import cromwell.engine.backend.jes.JesPendingExecutionHandle
import cromwell.logging.WorkflowLogger

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

object RetryingActor {
  sealed trait RetryingActorMessage
  final case class StartRetry[A](attemptsRemaining: Option[Int] = None,
                                 retryTimeLimit: Option[Duration] = None,
                                 priorValue: Option[A] = None) extends RetryingActorMessage

  def props[A](work: Option[A] => Future[A], backoff: ExponentialBackOff, isSuccess: A => Boolean, onSuccess: A => Any, onFailure: Throwable => Any): Props =
    Props(new RetryingActor(work, backoff, isSuccess, onSuccess, onFailure))
}

class RetriesExceededException(message: String) extends Exception(message)
class MaxTimeExceededException(message: String) extends Exception(message)

/**
  * Actor that retries a Future and messages back the parent when a success or
  * failure state is reached.
  *
  * `work` will be called at least once.
  *
  * The actor is initiated to retry
  *
  * @param work - Function that takes a prior value, Option[A], and produces a Future[A]
  * @param backoff - Exponential backoff algorithm
  * @param isSuccess - A => Boolean function to determine if the value from the completed
  *                  future (`work`) meets the criteria of being successful
  * @param onSuccess - A => Any function where the return value is the message that will be
  *                  sent back to the parent actor
  * @param onFailure - Throwable => Any function where the return value is the message that
  *                  will be sent back to the parent actor
  * @tparam A - The ultimate result of the `work` function
  */
class RetryingActor[A](work: Option[A] => Future[A],
                       backoff: ExponentialBackOff,
                       isSuccess: A => Boolean,
                       onSuccess: A => Any,
                       onFailure: Throwable => Any) extends Actor with CromwellActor {

  import RetryingActor._
  implicit val ec = context.system.dispatcher

  def sendRetryOrFailure(message: StartRetry[A], prior: Option[A]) = {
    val interval = backoff.nextBackOffMillis().millis
    val retryTimeLimitMillis = message.retryTimeLimit.map(_.toMillis.toInt)
    if (message.attemptsRemaining.contains(0)) {
        context.parent ! onFailure(new RetriesExceededException(s"Maximum retries exceeded"))
        context.stop(self)
      } else if (retryTimeLimitMillis.exists(max => (backoff.getElapsedTimeMillis + interval.toMillis) < max)) {
        context.parent ! onFailure(new MaxTimeExceededException(s"Maximum time exceeded"))
        context.stop(self)
      } else {
        context.system.scheduler.scheduleOnce(interval) {
          val msg = StartRetry(message.attemptsRemaining.map(_ - 1), message.retryTimeLimit, prior)
          self ! msg
        }
    }
  }

  def doWork(message: StartRetry[A]) = {
    work(message.priorValue) onComplete {
      case Success(value) if isSuccess(value) =>
        context.parent ! onSuccess(value)
        context.stop(self)
      case Success(value) =>
        sendRetryOrFailure(message, prior=Option(value))
      case Failure(ex) =>
        sendRetryOrFailure(message, prior=None)
    }
  }

  override def receive = LoggingReceive {
    case message: StartRetry[A] => doWork(message)
  }
}

object CallExecutionActor {
  sealed trait CallExecutionActorMessage
  final case class Finish(executionHandle: ExecutionHandle) extends CallExecutionActorMessage

  sealed trait ExecutionMode extends CallExecutionActorMessage {
    def execute(backendCall: BackendCall)(implicit ec: ExecutionContext): Future[ExecutionHandle]
  }

  case object Execute extends ExecutionMode {
    override def execute(backendCall: BackendCall)(implicit ec: ExecutionContext) = backendCall.execute
  }

  final case class Resume(jobKey: JobKey) extends ExecutionMode {
    override def execute(backendCall: BackendCall)(implicit ec: ExecutionContext) = backendCall.resume(jobKey)
  }

  def props(backendCall: BackendCall): Props = Props(new CallExecutionActor(backendCall))
}

/** Actor to manage the execution of a single call. */
class CallExecutionActor(backendCall: BackendCall) extends Actor with CromwellActor {
  import CallExecutionActor._

  val akkaLogger = Logging(context.system, classOf[CallExecutionActor])
  val logger = WorkflowLogger(
    "CallExecutionActor",
    backendCall.workflowDescriptor,
    akkaLogger = Option(akkaLogger),
    callTag = Option(backendCall.key.tag)
  )

  implicit val ec = context.system.dispatcher

  private def backoff = new ExponentialBackOff.Builder()
    .setInitialIntervalMillis(5.seconds.toMillis.toInt)
    .setMaxIntervalMillis(30.seconds.toMillis.toInt)
    .setMultiplier(1.1)
    .build()

  override def receive = LoggingReceive {
    case mode: ExecutionMode =>
      context.actorOf(RetryingActor.props(
        work = (prior: Option[ExecutionHandle]) => mode.execute(backendCall),
        backoff = backoff,
        isSuccess = (e: ExecutionHandle) => true,
        onSuccess = (e: ExecutionHandle) => Finish(e),
        onFailure = (f: Throwable) => Finish(FailedExecutionHandle(f))
      )) ! RetryingActor.StartRetry(attemptsRemaining=Option(5))

    case Finish(handle: JesPendingExecutionHandle) =>
      context.actorOf(RetryingActor.props(
        work = (prior: Option[ExecutionHandle]) => backendCall.poll(prior),
        backoff = backoff,
        isSuccess = (e: ExecutionHandle) => !e.isInstanceOf[JesPendingExecutionHandle],
        onSuccess = (e: ExecutionHandle) => Finish(e),
        onFailure = (f: Throwable) => Finish(FailedExecutionHandle(f))
      )) ! RetryingActor.StartRetry(priorValue=Option(handle))

    case Finish(handle) =>
      context.parent ! CallActor.ExecutionFinished(backendCall.call, handle.result)
      context.stop(self)

    case badMessage => logger.error(s"Unexpected message $badMessage.")
  }
}
