package cromwell.engine.workflow

import java.time.OffsetDateTime
import java.util.UUID

import akka.actor.{Actor, Props}
import akka.event.Logging
import cromwell.core.{WorkflowId, WorkflowSourceFiles}
import cromwell.database.obj.WorkflowMetadataKeys
import cromwell.engine.workflow.WorkflowStore.WorkflowToStart
import cromwell.engine.workflow.WorkflowStoreActor._
import cromwell.services.MetadataServiceActor.PutMetadataAction
import cromwell.services.{MetadataEvent, MetadataKey, MetadataValue, ServiceRegistryClient}

import scala.language.postfixOps
import scala.collection._

object WorkflowStore {
  sealed trait WorkflowState {
    def isStartable: Boolean = {
      this match {
        case x: StartableState => true
        case _ => false
      }
    }
  }

  case object Running extends WorkflowState

  sealed trait StartableState extends WorkflowState
  case object Submitted extends StartableState
  case object Restartable extends StartableState

  final case class SubmittedWorkflow(sources: WorkflowSourceFiles, state: WorkflowState) {
    def toWorkflowToStart(id: WorkflowId): WorkflowToStart = {
      state match {
        case r: StartableState => WorkflowToStart(id, sources, r)
        case _ => throw new IllegalArgumentException("This workflow is not currently in a startable state")
      }
    }
  }

  final case class WorkflowToStart(id: WorkflowId, sources: WorkflowSourceFiles, state: StartableState)
}

/**
  * Stores any submitted workflows which have not completed.
  *
  * This is not thread-safe, although it should not be being used in a multithreaded fashion. If that
  * changes, revisit.
  */
trait WorkflowStore {
  import WorkflowStore._

  /*
    The LinkedHashMap preserves insertion order which means that any requests for runnable workflows will
    be satisfied by taking the oldest available.
   */
  private var workflowStore = new mutable.LinkedHashMap[WorkflowId, SubmittedWorkflow]

  /**
    * Adds a WorkflowSourceFiles to the store and returns a WorkflowId for tracking purposes
    */
  def add(source: WorkflowSourceFiles): WorkflowId = {
    val workflowId = WorkflowId.randomId()
    workflowStore += (workflowId -> SubmittedWorkflow(source, Submitted))
    workflowId
  }

  /**
    * Provides a bulk add functionality. Adds all of the provided WorkflowSourceFiles at once, returning a List
    * of UUIDs matching the order of the supplied source files
    */
  def add(sources: Seq[WorkflowSourceFiles]): immutable.List[WorkflowId] = {
    val submittedWorkflows = sources.toList map { WorkflowId(UUID.randomUUID()) -> SubmittedWorkflow(_, Submitted) }
    workflowStore ++= submittedWorkflows
    submittedWorkflows map { _._1 }
  }

  /**
    * Retrieves up to n workflows which have not already been pulled into the engine and sets their pickedUp
    * flag to true
    */
  def fetchRunnableWorkflows(n: Int): immutable.List[WorkflowToStart] = {
    val startableWorkflows = workflowStore filter { case (k, v) => v.state.isStartable } take n
    workflowStore ++= startableWorkflows map { case (k, v) => k -> SubmittedWorkflow(v.sources, Running) }
    // Because we explicitly filtered out for startable states this is safe
    startableWorkflows map { case (k, v) => v.toWorkflowToStart(k) } toList
  }

  // FIXME: Do we care if it doesn't exist? If so to what extent
  def remove(id: WorkflowId): Unit = workflowStore -= id

  protected def dump: immutable.Map[WorkflowId, SubmittedWorkflow] = workflowStore.toMap
}

class WorkflowStoreActor extends WorkflowStore with Actor with ServiceRegistryClient {
  /*
    WARNING: WorkflowStore is NOT thread safe. Unless that statement is no longer true do NOT use threads
    outside of the the single threaded Actor event loop
   */

  private val logger = Logging(context.system, this)

  override def receive = {
    case SubmitWorkflow(source) =>
      val id = add(source)
      sendIdToMetadataService(id)
      sender ! WorkflowSubmitted(id)
    case SubmitWorkflows(sources) =>
      val ids = add(sources)
      ids foreach sendIdToMetadataService
      sender ! WorkflowsSubmitted(ids)
    case FetchRunnableWorkflows(n) => sender ! NewWorkflows(fetchRunnableWorkflows(n))
    case RemoveWorkflow(id) => remove(id)
  }

  /**
    * Takes the workflow id and sends it over to the metadata service w/ default empty values for inputs/outputs
    */
  private def sendIdToMetadataService(id: WorkflowId): Unit = {
    val submissionEvents = List(
      MetadataEvent(MetadataKey(id, None, WorkflowMetadataKeys.SubmissionTime), MetadataValue(OffsetDateTime.now.toString)),
      MetadataEvent.empty(MetadataKey(id, None, WorkflowMetadataKeys.Inputs)),
      MetadataEvent.empty(MetadataKey(id, None, WorkflowMetadataKeys.Outputs))
    )

    serviceRegistryActor ! PutMetadataAction(submissionEvents)
  }
}

object WorkflowStoreActor {
  sealed trait WorkflowStoreActorCommand
  case class SubmitWorkflow(source: WorkflowSourceFiles) extends WorkflowStoreActorCommand
  case class SubmitWorkflows(sources: Seq[WorkflowSourceFiles]) extends WorkflowStoreActorCommand
  case class FetchRunnableWorkflows(n: Int) extends WorkflowStoreActorCommand
  case class RemoveWorkflow(id: WorkflowId) extends WorkflowStoreActorCommand

  sealed trait WorkflowStoreActorResponse
  case class WorkflowSubmitted(workflowId: WorkflowId) extends WorkflowStoreActorResponse
  case class WorkflowsSubmitted(workflowIds: List[WorkflowId]) extends WorkflowStoreActorResponse
  case class NewWorkflows(workflows: immutable.List[WorkflowToStart]) extends WorkflowStoreActorResponse

  def props() = Props(new WorkflowStoreActor)
}
