package cromwell.jobstore

import cromwell.core.{JobOutput, JobOutputs, WorkflowId}
import cromwell.database.sql.JobStoreSqlDatabase
import cromwell.database.sql.tables.{JobStoreEntry, JobStoreResultSimpletonEntry}

import scala.concurrent.{ExecutionContext, Future}
import cromwell.core.ExecutionIndex._
import cromwell.core.simpleton.{WdlValueBuilder, WdlValueSimpleton}
import spray.json._
import cromwell.core.simpleton.WdlValueSimpleton._
import wdl4s.TaskOutput
import wdl4s.values.{WdlBoolean, WdlFloat, WdlInteger, WdlString}

class SqlJobStore(sqlDatabase: JobStoreSqlDatabase) extends JobStore {
  override def writeToDatabase(jobCompletions: Map[JobStoreKey, JobResult], workflowCompletions: List[WorkflowId])(implicit ec: ExecutionContext): Future[Unit] = {
    for {
      _ <- sqlDatabase.addJobStoreEntries(jobCompletions map toDatabase)
      _ <- sqlDatabase.removeJobResultsForWorkflows(workflowCompletions map {_.toString})
    } yield ()
  }

  private def toDatabase(jobCompletion: (JobStoreKey, JobResult)): (JobStoreEntry, Traversable[JobStoreResultSimpletonEntry]) = {
    jobCompletion match {
      case (key, JobResultSuccess(returnCode, jobOutputs)) =>
        val entry = JobStoreEntry(
          key.workflowId.toString,
          key.callFqn,
          key.index.fromIndex,
          key.attempt,
          jobSuccessful = true,
          returnCode,
          None,
          None)
        val simpletons = jobOutputs.mapValues(_.wdlValue).simplify.map {
          s => JobStoreResultSimpletonEntry(s.simpletonKey, s.simpletonValue.valueString, s.simpletonValue.wdlType.toString, -1) }
        (entry, simpletons)
      case (key, JobResultFailure(returnCode, throwable, retryable)) =>
        val entry = JobStoreEntry(
          key.workflowId.toString,
          key.callFqn,
          key.index.fromIndex,
          key.attempt,
          jobSuccessful = false,
          returnCode,
          Some(throwable.getMessage),
          Some(retryable))
        (entry, List.empty)
    }
  }

  private def toSimpleton(entry: JobStoreResultSimpletonEntry): WdlValueSimpleton = {
    val wdlValue = entry.wdlType match {
      case "WdlString" => WdlString(entry.simpletonValue)
      case "WdlInteger" => WdlInteger(entry.simpletonValue.toInt)
      case "WdlFloat" => WdlFloat(entry.simpletonValue.toFloat)
      case "WdlBoolean" => WdlBoolean(entry.simpletonValue.toBoolean)
      case _ => throw new RuntimeException("$entry: unrecognized WDL type: ${entry.wdlType}")
    }
    WdlValueSimpleton(entry.simpletonKey, wdlValue)
  }

  override def readJobResult(jobStoreKey: JobStoreKey, taskOutputs: Seq[TaskOutput])(implicit ec: ExecutionContext): Future[Option[JobResult]] = {
    sqlDatabase.fetchJobResult(jobStoreKey.workflowId.toString, jobStoreKey.callFqn, jobStoreKey.index.fromIndex, jobStoreKey.attempt) map {
      _ map { case (entry, simpletonEntries) =>
        entry match {
          case JobStoreEntry(_, _, _, _, true, returnCode, None, None, _) =>
            val simpletons = simpletonEntries map toSimpleton
            val wdlValues = WdlValueBuilder.build(taskOutputs, simpletons)
            wdlValues match {
              case scalaz.Success(v) => JobResultSuccess(returnCode, v mapValues JobOutput.apply)
              case scalaz.Failure(t) =>
                val message = "Error converting simpletons to WdlValues: " + t.list.mkString(", ")
                JobResultFailure(returnCode, new RuntimeException(message), retryable = false)
            }
          case JobStoreEntry(_, _, _, _, false, returnCode, Some(exceptionMessage), Some(retryable), _) =>
            JobResultFailure(returnCode, new Exception(exceptionMessage), retryable)
          case bad =>
            throw new Exception(s"Invalid contents of JobStore table: $bad")
        }
      }
    }
  }
}
