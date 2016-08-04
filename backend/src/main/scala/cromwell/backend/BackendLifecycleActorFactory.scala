package cromwell.backend

import java.nio.file.Path

import akka.actor.{ActorRef, Props}
import com.typesafe.config.Config
import cromwell.backend.io.WorkflowPaths
import cromwell.core.{ErrorOr, ExecutionStore, OutputStore, WorkflowOptions}
import wdl4s.Call
import wdl4s.expression.WdlStandardLibraryFunctions
import wdl4s.values.WdlValue

trait BackendLifecycleActorFactory {
  def workflowInitializationActorProps(workflowDescriptor: BackendWorkflowDescriptor,
                                       calls: Seq[Call],
                                       serviceRegistryActor: ActorRef): Option[Props]

  def jobExecutionActorProps(jobDescriptor: BackendJobDescriptor,
                             initializationData: Option[BackendInitializationData],
                             serviceRegistryActor: ActorRef): Props

  def workflowFinalizationActorProps(workflowDescriptor: BackendWorkflowDescriptor,
                                     calls: Seq[Call],
                                     executionStore: ExecutionStore,
                                     outputStore: OutputStore,
                                     initializationData: Option[BackendInitializationData]): Option[Props] = None

  def expressionLanguageFunctions(workflowDescriptor: BackendWorkflowDescriptor,
                                  jobKey: BackendJobDescriptorKey,
                                  initializationData: Option[BackendInitializationData]): WdlStandardLibraryFunctions

  def getExecutionRootPath(workflowDescriptor: BackendWorkflowDescriptor, backendConfig: Config, initializationData: Option[BackendInitializationData]): Path = {
      new WorkflowPaths(workflowDescriptor, backendConfig).executionRoot
  }

  def coerceDefaultRuntimeAttributes(options: WorkflowOptions): ErrorOr[Map[String, WdlValue]]
}
