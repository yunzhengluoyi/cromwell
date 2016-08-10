package cromwell.backend

import java.nio.file.Path

import akka.actor.{ActorRef, ActorSystem, Props}
import com.typesafe.config.Config
import cromwell.backend.callcaching.FileContentsHasherActor
import cromwell.backend.callcaching.FileContentsHasherActor.FileHashingFunction
import cromwell.backend.io.WorkflowPaths
import cromwell.core.{ExecutionStore, OutputStore}
import wdl4s.Call
import wdl4s.expression.WdlStandardLibraryFunctions

trait BackendLifecycleActorFactory {

  def actorSystem: ActorSystem

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

  def runtimeAttributeDefinitions: Set[RuntimeAttributeDefinition] = Set.empty

  lazy val fileHashingFunction: Option[FileHashingFunction] = None

  final lazy val fileContentsHasherActor: ActorRef = actorSystem.actorOf(FileContentsHasherActor.props(fileHashingFunction))
}
