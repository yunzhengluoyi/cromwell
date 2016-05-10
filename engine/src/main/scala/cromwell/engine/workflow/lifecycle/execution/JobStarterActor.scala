package cromwell.engine.workflow.lifecycle.execution

import akka.actor.{Actor, Props}
import cromwell.backend._
import cromwell.engine.EngineWorkflowDescriptor
import cromwell.engine.workflow.lifecycle.execution.JobStarterActor.{BackendJobStartFailed, BackendJobStartSucceeded, Start}

object JobStarterActor {
  case object Start
  case class BackendJobStartSucceeded(jobDescriptor: BackendJobDescriptor, props: Props)
  case class BackendJobStartFailed(jobKey: JobKey, throwable: Throwable)

  def props(executionData: WorkflowExecutionActorData,
            jobKey: BackendJobDescriptorKey,
            factory: BackendLifecycleActorFactory,
            configDescriptor: BackendConfigurationDescriptor) = Props(new JobStarterActor(executionData, jobKey, factory, configDescriptor))
}

case class JobStarterActor(executionData: WorkflowExecutionActorData,
                           jobKey: BackendJobDescriptorKey,
                           factory: BackendLifecycleActorFactory,
                           configDescriptor: BackendConfigurationDescriptor) extends Actor with InputEvaluation {

  override def workflowDescriptor: EngineWorkflowDescriptor = executionData.workflowDescriptor
  override def executionStore: ExecutionStore = executionData.executionStore
  override def outputStore: OutputStore = executionData.outputStore

  override def receive = {
    case Start => resolveAndEvaluate(jobKey, factory.expressionLanguageFunctions(workflowDescriptor.backendDescriptor, jobKey, configDescriptor)) map { inputs =>
      val jobDescriptor = BackendJobDescriptor(workflowDescriptor.backendDescriptor, jobKey, inputs)
       val props = factory.jobExecutionActorProps(
          jobDescriptor,
          BackendConfigurationDescriptor(configDescriptor.backendConfig, configDescriptor.globalConfig)
        )
      context.parent ! BackendJobStartSucceeded(jobDescriptor, props)
    } recover {
      case t => context.parent ! BackendJobStartFailed(jobKey, t)
    }

    context stop self
  }
}
