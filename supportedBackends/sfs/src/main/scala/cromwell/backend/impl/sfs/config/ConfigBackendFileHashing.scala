package cromwell.backend.impl.sfs.config

import java.io.{File, FileInputStream}

import akka.event.LoggingAdapter
import cromwell.backend.callcaching.FileHasherWorkerActor.SingleFileHashRequest

import scala.util.{Failure, Success, Try}

private[config] object ConfigBackendFileHashing {
  def getMd5Result(request: SingleFileHashRequest, log: LoggingAdapter) = {
    val md5Result = for {
      fileInputStream <- Try(new FileInputStream(new File(request.file.valueString)))
      // Use = here so that closeAndLog runs even if this fails:
      md5String = Try(org.apache.commons.codec.digest.DigestUtils.md5Hex(fileInputStream))
      _ = closeAndLog(fileInputStream, log)
    } yield md5String

    // Because we used = above, we must flatten the try down here
    md5Result.flatten
  }

  def closeAndLog(fileInputStream: FileInputStream, log: LoggingAdapter) = {
    Try(fileInputStream.close()) match {
      case Success(_) => // No need to log a success!
      case Failure(t) => log.error(s"Could not close file stream: $t")
    }
  }
}
