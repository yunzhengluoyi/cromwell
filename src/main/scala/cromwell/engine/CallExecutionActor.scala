package cromwell.engine

import akka.actor.{Actor, Props}
import akka.event.{Logging, LoggingReceive}
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.util.ExponentialBackOff
import cromwell.engine.RetryingActor.StartRetry
import cromwell.engine.backend._
import cromwell.engine.backend.jes.{JesPendingExecutionHandle, JesRetryableExecutionHandle}
import cromwell.logging.WorkflowLogger

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Try, Failure, Success}

object RetryingActor {
  sealed trait RetryingActorMessage
  final case class StartRetry[A](retries: Option[Int] = None,
                                 retryTimeLimit: Option[Duration] = None,
                                 elapsedMillis: Long = 0) extends RetryingActorMessage

  def props[A](work: A => Future[A], backoff: ExponentialBackOff, isSuccess: A => Boolean, onSuccess: A => Any, onFailure: Throwable => Any): Props =
    Props(new RetryingActor(work, backoff, isSuccess, onSuccess, onFailure))
}

class RetriesExceededException(message: String) extends Exception(message)

class RetryingActor[A](work: A => Future[A],
                       backoff: ExponentialBackOff,
                       isSuccess: A => Boolean,
                       onSuccess: A => Any,
                       onFailure: Throwable => Any) extends Actor with CromwellActor {

  implicit val ec = context.system.dispatcher

  def sendRetryOrFailure(message: StartRetry[A], prior: Option[A]) = {
    val retries = message.retries.map(_ - 1)
    if (retries.contains(0)) {
      context.parent ! onFailure(new RetriesExceededException(s"Max retries exceeded"))
      context.stop(self)
    } else {
      val interval = backoff.nextBackOffMillis().millis
      context.system.scheduler.scheduleOnce(interval) {
        println(s"Sending StartRetry: $interval -- ${message.retries}")
        self ! StartRetry(message.retries.map(_ - 1), message.retryTimeLimit, message.elapsedMillis + interval.toMillis)
      }
    }
  }

  def doWork(message: StartRetry[A]) = {
    work(message.priorValue) onComplete {
      case Success(value) if isSuccess(value) =>
        println(s"RetryingActor: Success")
        context.parent ! onSuccess(value)
        context.stop(self)
      case Success(value) =>
        println(s"RetryingActor: Success (isSuccess = false)")
        sendRetryOrFailure(message, prior=Option(value))
      case Failure(ex) =>
        println(s"RetryingActor: FAILURE: ${ex.getMessage}")
        sendRetryOrFailure(message, prior=None)
    }
  }

  override def receive = LoggingReceive {
    case message: StartRetry[A] =>
      message.retries match {
        case Some(n) if n > 0 => doWork(message)
        case Some(n) => context.parent ! Unit
        case None => doWork(message)
      }
  }
}

object CallExecutionActor {
  sealed trait CallExecutionActorMessage
  final case class IssuePollRequest(executionHandle: ExecutionHandle) extends CallExecutionActorMessage
  final case class PollResponseReceived(executionHandle: ExecutionHandle) extends CallExecutionActorMessage
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

  private def backoffWithMaxElapsedTime(maxTimeMillis: Int) = new ExponentialBackOff.Builder()
    .setInitialIntervalMillis(5.seconds.toMillis.toInt)
    .setMaxIntervalMillis(30.seconds.toMillis.toInt)
    .setMaxElapsedTimeMillis(maxTimeMillis)
    .setMultiplier(1.1)
    .build()

  override def receive = LoggingReceive {
    case mode: ExecutionMode =>
      context.actorOf(RetryingActor.props(
        work = (prior: ExecutionHandle) => mode.execute(backendCall),
        backoff = backoffWithMaxElapsedTime(5.minutes.toMillis.toInt),
        isSuccess = (e: ExecutionHandle) => true,
        onSuccess = (e: ExecutionHandle) => Finish(e),
        onFailure = (f: Throwable) => Finish(FailedExecutionHandle(f))
      )) ! RetryingActor.StartRetry(retries=Option(5))

    case Finish(handle: JesPendingExecutionHandle) =>
      println(s"CallExecutionActor: JesPendingExecutionHandle found")
      context.actorOf(RetryingActor.props(
        work = (prior: ExecutionHandle) => backendCall.poll(prior),
        backoff = backoffWithMaxElapsedTime(Integer.MAX_VALUE),
        isSuccess = (e: ExecutionHandle) => !e.isInstanceOf[JesPendingExecutionHandle],
        onSuccess = (e: ExecutionHandle) => Finish(e),
        onFailure = (f: Throwable) => Finish(FailedExecutionHandle(f))
      )) ! RetryingActor.StartRetry()

    case Finish(handle) =>
      println(s"CallExecutionActor: Finish message received ${handle}")
      context.parent ! CallActor.ExecutionFinished(backendCall.call, handle.result)
      context.stop(self)

    case badMessage => logger.error(s"Unexpected message $badMessage.")
  }
}
