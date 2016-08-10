package cromwell.backend.callcaching

import java.util.UUID

import akka.actor.{Actor, ActorLogging, Props, Terminated}
import akka.event.LoggingAdapter
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import cromwell.backend.BackendInitializationData
import cromwell.backend.callcaching.FileContentsHasherActor._
import cromwell.core.JobKey
import cromwell.core.callcaching._
import wdl4s.values.WdlFile

import scala.util.{Failure, Success, Try}

class FileContentsHasherActor(workerFunction: Option[FileHashingFunction]) extends Actor with ActorLogging {
  def makeWorker = context.actorOf(FileHasherWorkerActor.props(workerFunction), s"FileHasherWorkerActor-${UUID.randomUUID.toString.substring(0,7)}")

  var router = {
    val routees = Vector.fill(100) {
      val r = makeWorker
      context watch r
      ActorRefRoutee(r)
    }
    Router(RoundRobinRoutingLogic(), routees)
  }

  def receive = {
    case w: SingleFileHashRequest =>
      router.route(w, sender())
    case Terminated(a) =>
      router = router.removeRoutee(a)
      val r = makeWorker
      context watch r
      router = router.addRoutee(r)
  }
}

object FileContentsHasherActor {
  def props(workerFunction: Option[FileHashingFunction]): Props = Props(new FileContentsHasherActor(workerFunction))
  case class FileHashingFunction(work: (SingleFileHashRequest, LoggingAdapter) => Try[String])

  sealed trait BackendSpecificHasherCommand { def jobKey: JobKey }
  case class SingleFileHashRequest(jobKey: JobKey, hashKey: HashKey, file: WdlFile, initializationData: Option[BackendInitializationData]) extends BackendSpecificHasherCommand
  case class HashesNoLongerRequired(jobKey: JobKey) extends BackendSpecificHasherCommand

  sealed trait BackendSpecificHasherResponse extends SuccessfulHashResultMessage
  case class FileHashResponse(hashResult: HashResult) extends BackendSpecificHasherResponse { override def hashes = Set(hashResult) }
}

/**
  * Blocking worker. Warning! If this actor dies then its mailbox of hash requests will be lost
  */
private[callcaching] class FileHasherWorkerActor(workerFunction: Option[FileHashingFunction]) extends Actor with ActorLogging {
  override def receive = {
    case x: SingleFileHashRequest =>

      // Create the path with the filesystem in the initialization data:
      workerFunction.map(_.work(x, log)) match {
        case Some(Success(crc32cSuccess)) => sender ! FileHashResponse(HashResult(x.hashKey, HashValue(crc32cSuccess)))
        case Some(Failure(t)) => sender ! HashingFailedMessage(x.hashKey, t)
        case None => sender ! HashingFailedMessage(x.hashKey, new NotImplementedError("Backend has no file hashing function"))
      }
    case x => log.error(s"Unexpected message to ${self.path.name}: $x")
  }
}

private[callcaching] object FileHasherWorkerActor {
  def props(workerFunction: Option[FileHashingFunction]): Props = Props(new FileHasherWorkerActor(workerFunction))
}