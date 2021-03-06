package cromwell.services.metadata.impl

import java.time.OffsetDateTime

import akka.actor.{ActorRef, LoggingFSM, Props}
import com.typesafe.config.ConfigFactory
import cromwell.database.CromwellDatabase
import cromwell.services.metadata.impl.MetadataSummaryRefreshActor._

import scala.util.{Failure, Success}


/**
  * This looks for workflows whose metadata summaries are in need of refreshing and refreshes those summaries.
  * Despite its package location this is not actually a service, but a type of actor spawned at the behest of
  * the MetadataServiceActor.
  *
  */

object MetadataSummaryRefreshActor {
  sealed trait MetadataSummaryActorMessage
  final case class SummarizeMetadata(respondTo: ActorRef) extends MetadataSummaryActorMessage
  case object MetadataSummarySuccess extends MetadataSummaryActorMessage
  final case class MetadataSummaryFailure(t: Throwable) extends MetadataSummaryActorMessage

  def props(startMetadataTimestamp: Option[OffsetDateTime]) = Props(new MetadataSummaryRefreshActor(startMetadataTimestamp))

  trait SummaryRefreshState
  case object WaitingForRequest extends SummaryRefreshState
  case object SummarizingMetadata extends SummaryRefreshState
  private case class MetadataSummaryComplete(startMetadataId: Long) extends SummaryRefreshState

  final case class SummaryRefreshData(startMetadataId: Long)
}


class MetadataSummaryRefreshActor(startMetadataTimestamp: Option[OffsetDateTime]) extends LoggingFSM[SummaryRefreshState, SummaryRefreshData] with MetadataDatabaseAccess with CromwellDatabase {

  val config = ConfigFactory.load
  implicit val ec = context.dispatcher

  startWith(WaitingForRequest, SummaryRefreshData(startMetadataId = 0L))

  when (WaitingForRequest) {
    case (Event(SummarizeMetadata(respondTo), data)) =>
      val startMetadataId = data.startMetadataId
      refreshWorkflowMetadataSummaries(startMetadataId, startMetadataTimestamp) onComplete {
        case Success(id) =>
          respondTo ! MetadataSummarySuccess
          self ! MetadataSummaryComplete(startMetadataId = id + 1)
        case Failure(t) =>
          log.error(t, "Failed to summarize metadata starting from index {}", startMetadataId)
          respondTo ! MetadataSummaryFailure(t)
          self ! MetadataSummaryComplete(startMetadataId = startMetadataId)
      }
      goto(SummarizingMetadata)
  }

  when (SummarizingMetadata) {
    case Event(MetadataSummaryComplete(startMetadataId), _) =>
      goto(WaitingForRequest) using SummaryRefreshData(startMetadataId)
  }

  whenUnhandled {
    case Event(wut, _) =>
      log.warning("Unrecognized or unexpected message while in state '{}': {}", stateName, wut)
      stay()
  }
}
