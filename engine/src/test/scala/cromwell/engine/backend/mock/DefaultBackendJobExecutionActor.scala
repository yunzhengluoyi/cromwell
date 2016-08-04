package cromwell.engine.backend.mock

import akka.actor.{ActorRef, Props}
import cromwell.backend.BackendJobExecutionActor.{BackendJobExecutionResponse, SucceededResponse}
import cromwell.backend._
import cromwell.core.{ErrorOr, WorkflowOptions}
import wdl4s.Call
import wdl4s.expression.{NoFunctions, WdlStandardLibraryFunctions}
import wdl4s.values.WdlValue

import scala.concurrent.Future

import scalaz._
import Scalaz._

object DefaultBackendJobExecutionActor {
  def props(jobDescriptor: BackendJobDescriptor, configurationDescriptor: BackendConfigurationDescriptor) = Props(DefaultBackendJobExecutionActor(jobDescriptor, configurationDescriptor))
}

case class DefaultBackendJobExecutionActor(override val jobDescriptor: BackendJobDescriptor, override val configurationDescriptor: BackendConfigurationDescriptor) extends BackendJobExecutionActor {
  override def execute: Future[BackendJobExecutionResponse] = {
    Future.successful(SucceededResponse(jobDescriptor.key, Some(0), (jobDescriptor.call.task.outputs map taskOutputToJobOutput).toMap))
  }
  override def recover = execute

  override def abort(): Unit = ()
}

class DefaultBackendLifecycleActorFactory(configurationDescriptor: BackendConfigurationDescriptor) extends BackendLifecycleActorFactory {
  override def workflowInitializationActorProps(workflowDescriptor: BackendWorkflowDescriptor,
                                                calls: Seq[Call],
                                                serviceRegistryActor: ActorRef): Option[Props] = None

  override def jobExecutionActorProps(jobDescriptor: BackendJobDescriptor,
                                      initializationData: Option[BackendInitializationData],
                                      serviceRegistryActor: ActorRef): Props = {
    DefaultBackendJobExecutionActor.props(jobDescriptor, configurationDescriptor)
  }

  override def expressionLanguageFunctions(workflowDescriptor: BackendWorkflowDescriptor,
                                           jobKey: BackendJobDescriptorKey,
                                           initializationData: Option[BackendInitializationData]): WdlStandardLibraryFunctions = NoFunctions

  override def coerceDefaultRuntimeAttributes(options: WorkflowOptions): ErrorOr[Map[String, WdlValue]] = Map.empty[String, WdlValue].successNel
}

