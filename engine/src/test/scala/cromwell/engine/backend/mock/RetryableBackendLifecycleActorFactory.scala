package cromwell.engine.backend.mock

import akka.actor.{ActorRef, Props}
import cromwell.backend._
import cromwell.core.{ErrorOr, WorkflowOptions}
import wdl4s.Call
import wdl4s.expression.{NoFunctions, WdlStandardLibraryFunctions}
import wdl4s.values.WdlValue
import scalaz._
import Scalaz._

class RetryableBackendLifecycleActorFactory(configurationDescriptor: BackendConfigurationDescriptor) extends BackendLifecycleActorFactory {
  override def workflowInitializationActorProps(workflowDescriptor: BackendWorkflowDescriptor,
                                                calls: Seq[Call],
                                                serviceRegistryActor: ActorRef): Option[Props] = None

  override def jobExecutionActorProps(jobDescriptor: BackendJobDescriptor,
                                      initializationData: Option[BackendInitializationData],
                                      serviceRegistryActor: ActorRef): Props = {
    RetryableBackendJobExecutionActor.props(jobDescriptor, configurationDescriptor)
  }

  override def expressionLanguageFunctions(workflowDescriptor: BackendWorkflowDescriptor,
                                           jobKey: BackendJobDescriptorKey,
                                           initializationData: Option[BackendInitializationData]): WdlStandardLibraryFunctions = NoFunctions

  override def coerceDefaultRuntimeAttributes(options: WorkflowOptions): ErrorOr[Map[String, WdlValue]] = Map.empty[String, WdlValue].successNel
}
