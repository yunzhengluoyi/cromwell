package cromwell.backend.callcaching

import akka.actor.{Actor, ActorLogging, Props}
import akka.event.LoggingAdapter
import cromwell.backend.BackendInitializationData
import cromwell.core.JobKey
import cromwell.core.callcaching._
import wdl4s.values.WdlFile
import FileHasherWorkerActor._

import scala.util.{Failure, Success, Try}

/**
  * Blocking worker. Warning! If this actor dies then its mailbox of hash requests will be lost
  */
class FileHasherWorkerActor(workerFunction: Option[FileHashingFunction]) extends Actor with ActorLogging {
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

object FileHasherWorkerActor {
  def props(workerFunction: Option[FileHashingFunction]): Props = Props(new FileHasherWorkerActor(workerFunction))

  case class FileHashingFunction(work: (SingleFileHashRequest, LoggingAdapter) => Try[String])

  sealed trait BackendSpecificHasherCommand { def jobKey: JobKey }
  case class SingleFileHashRequest(jobKey: JobKey, hashKey: HashKey, file: WdlFile, initializationData: Option[BackendInitializationData]) extends BackendSpecificHasherCommand
  case class HashesNoLongerRequired(jobKey: JobKey) extends BackendSpecificHasherCommand

  sealed trait BackendSpecificHasherResponse extends SuccessfulHashResultMessage
  case class FileHashResponse(hashResult: HashResult) extends BackendSpecificHasherResponse { override def hashes = Set(hashResult) }
}