package cromwell.engine.workflow.lifecycle.execution

import akka.actor.{FSM, LoggingFSM, Props}
import com.typesafe.config.ConfigFactory
import cromwell.backend.BackendJobExecutionActor.{BackendJobExecutionFailedResponse, BackendJobExecutionFailedRetryableResponse, BackendJobExecutionSucceededResponse, ExecuteJobCommand}
import cromwell.backend._
import cromwell.core.{WorkflowId, _}
import cromwell.engine.ExecutionIndex._
import cromwell.engine.ExecutionStatus.NotStarted
import cromwell.engine.backend.{BackendConfiguration, CromwellBackends}
import cromwell.engine.workflow.lifecycle.execution.WorkflowExecutionActor.WorkflowExecutionActorState
import cromwell.engine.{CromwellWdlFunctions, EngineWorkflowDescriptor, ExecutionStatus}
import lenthall.exception.ThrowableAggregation
import wdl4s._
import wdl4s.util.TryUtil
import wdl4s.values.{WdlArray, WdlValue}

import scala.annotation.tailrec
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object WorkflowExecutionActor {

  /**
    * States
    */
  sealed trait WorkflowExecutionActorState { def terminal = false }
  sealed trait WorkflowExecutionActorTerminalState extends WorkflowExecutionActorState { override val terminal = true }

  case object WorkflowExecutionPendingState extends WorkflowExecutionActorState
  case object WorkflowExecutionInProgressState extends WorkflowExecutionActorState
  case object WorkflowExecutionSuccessfulState extends WorkflowExecutionActorTerminalState
  case object WorkflowExecutionFailedState extends WorkflowExecutionActorTerminalState
  case object WorkflowExecutionAbortedState extends WorkflowExecutionActorTerminalState

  /**
    * Commands
    */
  sealed trait WorkflowExecutionActorCommand
  case object StartExecutingWorkflowCommand extends WorkflowExecutionActorCommand
  case object RestartExecutingWorkflowCommand extends WorkflowExecutionActorCommand
  case object AbortExecutingWorkflowCommand extends WorkflowExecutionActorCommand

  /**
    * Responses
    */
  sealed trait WorkflowExecutionActorResponse
  case object WorkflowExecutionSucceededResponse extends WorkflowExecutionActorResponse
  case object WorkflowExecutionAbortedResponse extends WorkflowExecutionActorResponse
  final case class WorkflowExecutionFailedResponse(reasons: Seq[Throwable]) extends WorkflowExecutionActorResponse

  /**
    * Internal control flow messages
    */
  private case class JobInitializationFailed(throwable: Throwable)
  private case class ScatterCollectionFailedResponse(collectorKey: CollectorKey, throwable: Throwable)
  private case class ScatterCollectionSucceededResponse(collectorKey: CollectorKey, outputs: CallOutputs)

  /**
    * Internal ADTs
    */
  // Represents a scatter in the execution store. Index is here in prevision for nested scatters
  case class ScatterKey(scope: Scatter, index: ExecutionIndex) extends JobKey {
    override val attempt = 1
    override val tag = scope.unqualifiedName

    /**
      * Creates a sub-ExecutionStore with Starting entries for each of the scoped children.
      *
      * @param count Number of ways to scatter the children.
      * @return ExecutionStore of scattered children.
      */
    def populate(count: Int): Map[JobKey, ExecutionStatus.Value] = {
      val keys = this.scope.children flatMap { explode(_, count) }
      keys map { _ -> ExecutionStatus.NotStarted } toMap
    }

    private def explode(scope: Scope, count: Int): Seq[JobKey] = {
      scope match {
        case call: Call =>
          val shards = (0 until count) map { i => BackendJobDescriptorKey(call, Option(i), 1) }
          shards :+ CollectorKey(call)
        case scatter: Scatter =>
          throw new UnsupportedOperationException("Nested Scatters are not supported (yet).")
        case e =>
          throw new UnsupportedOperationException(s"Scope ${e.getClass.getName} is not supported.")
      }
    }
  }

  // Represents a scatter collection for a call in the execution store
  case class CollectorKey(scope: Call) extends JobKey {
    override val index = None
    override val attempt = 1
    override val tag = s"Collector-${scope.unqualifiedName}"
  }

  case class WorkflowExecutionException(override val throwables: List[Throwable]) extends ThrowableAggregation {
    override val exceptionContext = s"WorkflowExecutionActor"
  }

  def props(workflowId: WorkflowId, workflowDescriptor: EngineWorkflowDescriptor): Props = Props(WorkflowExecutionActor(workflowId, workflowDescriptor))
}

final case class WorkflowExecutionActor(workflowId: WorkflowId, workflowDescriptor: EngineWorkflowDescriptor) extends LoggingFSM[WorkflowExecutionActorState, WorkflowExecutionActorData] {

  import WorkflowExecutionActor._
  import lenthall.config.ScalaConfig._

  val tag = self.path.name
  private lazy val DefaultMaxRetriesFallbackValue = 10

  // TODO: We should probably create a trait which loads all the configuration (once per application), and let classes mix it in
  // to avoid doing ConfigFactory.load() at multiple places
  val MaxRetries = ConfigFactory.load().getIntOption("system.max-retries") match {
    case Some(value) => value
    case None =>
      log.warning(s"Failed to load the max-retries value from the configuration. Defaulting back to a value of `$DefaultMaxRetriesFallbackValue`.")
      DefaultMaxRetriesFallbackValue
  }
  private val workflow = workflowDescriptor.backendDescriptor.workflowNamespace.workflow
  private val factories = TryUtil.sequenceMap(workflowDescriptor.backendAssignments.values.toSeq map { backendName =>
    backendName -> CromwellBackends.shadowBackendLifecycleFactory(backendName)
  } toMap) recover {
    case e => throw new RuntimeException("Could not instantiate backend factories", e)
  } get

  private val configs = TryUtil.sequenceMap(workflowDescriptor.backendAssignments.values.toSeq map { backendName =>
    backendName -> BackendConfiguration.backendConfigurationDescriptor(backendName)
  } toMap) recover {
    case e => throw new RuntimeException("Could not instantiate backend configurations", e)
  } get

  // Initialize the StateData with ExecutionStore (all calls as NotStarted) and SymbolStore
  startWith(
    WorkflowExecutionPendingState,
    WorkflowExecutionActorData(
      workflowDescriptor,
      executionStore = buildExecutionStore(),
      outputStore = OutputStore.empty))

  def buildExecutionStore(): ExecutionStore = {
    val workflow = workflowDescriptor.backendDescriptor.workflowNamespace.workflow
    // Only add direct children to the store, the rest is dynamically created when necessary
    val callExecutions = Scope.collectCalls(workflow.children) map { BackendJobDescriptorKey(_, None, 1) -> NotStarted }
    val scatterExecutions = Scope.collectScatters(workflow.children) map { ScatterKey(_, None) -> NotStarted }

    ExecutionStore((scatterExecutions ++ callExecutions) toMap)
  }

  when(WorkflowExecutionPendingState) {
    case Event(StartExecutingWorkflowCommand, stateData) =>
      val data = startRunnableScopes(stateData)
      goto(WorkflowExecutionInProgressState) using data
    case Event(RestartExecutingWorkflowCommand, _) =>
      // TODO: Restart executing
      goto(WorkflowExecutionInProgressState)
    case Event(AbortExecutingWorkflowCommand, _) =>
      context.parent ! WorkflowExecutionAbortedResponse
      goto(WorkflowExecutionAbortedState)
  }

  when(WorkflowExecutionInProgressState) {
    case Event(BackendJobExecutionSucceededResponse(jobKey, callOutputs), stateData) =>
      handleCallSuccessful(jobKey, callOutputs, stateData)
    case Event(BackendJobExecutionFailedResponse(jobKey, reason), stateData) =>
      log.warning(s"Job ${jobKey.call.fullyQualifiedName} failed! Reason: ${reason.getMessage}", reason)
      goto(WorkflowExecutionFailedState) using stateData.mergeExecutionDiff(WorkflowExecutionDiff(Map(jobKey -> ExecutionStatus.Failed)))
    case Event(BackendJobExecutionFailedRetryableResponse(jobKey, reason), stateData) =>
      log.warning(s"Job ${jobKey.tag} failed with a retryable failure: ${reason.getMessage}")
      handleRetryableFailure(jobKey)
    case Event(JobInitializationFailed(reason), stateData) =>
      log.warning(s"Jobs failed to initialize: $reason")
      goto(WorkflowExecutionFailedState)
    case Event(ScatterCollectionSucceededResponse(jobKey, callOutputs), stateData) =>
      handleCallSuccessful(jobKey, callOutputs, stateData)
    case Event(BackendJobExecutionFailedResponse(jobKey, reason), stateData) =>
      log.warning(s"Job ${jobKey.tag} failed: $reason")
      goto(WorkflowExecutionFailedState)
    case Event(AbortExecutingWorkflowCommand, stateData) => ??? // TODO: Implement!
    case Event(_, _) => ??? // TODO: Lots of extra stuff to include here...
  }

  when(WorkflowExecutionSuccessfulState) {
    FSM.NullFunction
  }
  when(WorkflowExecutionFailedState) {
    FSM.NullFunction
  }
  when(WorkflowExecutionAbortedState) {
    FSM.NullFunction
  }

  whenUnhandled {
    case unhandledMessage =>
      log.warning(s"$tag received an unhandled message: $unhandledMessage in state: $stateName")
      stay
  }

  onTransition {
    case _ -> toState if toState.terminal =>
      log.info(s"$tag done. Shutting down.")
      context.stop(self)
    case fromState -> toState =>
      log.info(s"$tag transitioning from $fromState to $toState.")
  }

  private def handleRetryableFailure(jobKey: BackendJobDescriptorKey) = {
    // We start with index 1 for #attempts, hence invariant breaks only if jobKey.attempt > MaxRetries
    if (jobKey.attempt <= MaxRetries) {
      val newJobKey = jobKey.copy(attempt = jobKey.attempt + 1)
      log.info(s"Retrying job execution for ${newJobKey.tag}")
      /** Currently, we update the status of the old key to Preempted, and add a new entry (with the #attempts incremented by 1)
        * to the execution store with status as NotStarted. This allows startRunnableCalls to re-execute this job */
      val executionDiff = WorkflowExecutionDiff(Map(jobKey -> ExecutionStatus.Preempted, newJobKey -> ExecutionStatus.NotStarted))
      val newData = stateData.mergeExecutionDiff(executionDiff)
      stay() using startRunnableScopes(newData)
    } else {
      log.warning(s"Exhausted maximum number of retries for job ${jobKey.tag}. Failing.")
      goto(WorkflowExecutionFailedState) using stateData.mergeExecutionDiff(WorkflowExecutionDiff(Map(jobKey -> ExecutionStatus.Failed)))
    }
  }

  def handleCallSuccessful(jobKey: JobKey, outputs: CallOutputs, data: WorkflowExecutionActorData) = {
    log.info(s"Job ${jobKey.tag} succeeded! Outputs: ${outputs.mkString("\n")}")
    val newData = data.jobExecutionSuccess(jobKey, outputs)

    if (newData.isWorkflowComplete) {
      log.info(newData.outputsJson())
      goto(WorkflowExecutionSuccessfulState) using newData
    }
    else
      stay() using startRunnableScopes(newData)
  }

  /**
    * Attempt to start all runnable jobs and return updated state data.  This will create a new copy
    * of the state data including new pending persists.
    */
  @tailrec
  private def startRunnableScopes(data: WorkflowExecutionActorData): WorkflowExecutionActorData = {
    val runnableScopes = data.executionStore.runnableScopes
    val runnableCalls = runnableScopes collect { case c: Call => c.fullyQualifiedName }
    if (runnableCalls.nonEmpty) log.info(s"Starting calls: " + runnableCalls.mkString(", "))

    val dataDiffs = runnableScopes map {
      case k: BackendJobDescriptorKey => processRunnableJob(k, data)
      case k: ScatterKey => processRunnableScatter(k, data)
      case k: CollectorKey => processRunnableCollector(k, data)
      case k => Failure(new UnsupportedOperationException(s"Unknown entry in execution store: ${k.tag}"))
    }

    TryUtil.sequence(dataDiffs.toSeq) match {
      case Success(diffs) if diffs.exists(_.containsNewEntry) => startRunnableScopes(data.mergeExecutionDiffs(diffs))
      case Success(diffs) => data.mergeExecutionDiffs(diffs)
      case Failure(e) =>
        self ! JobInitializationFailed(e)
        data
    }
  }

  private def processRunnableJob(jobKey: BackendJobDescriptorKey, data: WorkflowExecutionActorData): Try[WorkflowExecutionDiff] = {
    workflowDescriptor.backendAssignments.get(jobKey.call) match {
      case None =>
        val message = s"Could not start call ${jobKey.tag} because it was not assigned a backend"
        log.error(s"$tag $message")
        throw new IllegalStateException(s"$tag $message")
      case Some(backendName) =>
        (configs.get(backendName), factories.get(backendName)) match {
          case (Some(configDescriptor), Some(factory)) =>
            startJob(jobKey, data, configDescriptor, factory)
          case (c, f) =>
            val noConf = if (c.isDefined) None else Option(new Exception(s"Could not get BackendConfigurationDescriptor for backend $backendName"))
            val noFactory = if (f.isDefined) None else Option(new Exception(s"Could not get BackendLifecycleActor for backend $backendName"))
            val errors = List(noConf, noFactory).flatten
            errors foreach(error => log.error(error.getMessage, error))
            throw new WorkflowExecutionException(errors)
        }
    }
  }

  /** PBE: the return value of WorkflowExecutionActorState is just temporary.
    *      This should probably return a Try[BackendJobDescriptor], Unit, Boolean,
    *      Try[ActorRef], or something to indicate if the job was started
    *      successfully.  Or, if it can fail to start, some indication of why it
    *      failed to start
    */
  private def startJob(jobKey: BackendJobDescriptorKey,
                       data: WorkflowExecutionActorData,
                       configDescriptor: BackendConfigurationDescriptor,
                       factory: BackendLifecycleActorFactory): Try[WorkflowExecutionDiff] = {
    // FIXME This is a potential bottleneck as input evaluation can be arbitrarily long and executes code coming from the backend.
    // It should probably be actorified / isolated
    data.resolveAndEvaluate(jobKey, factory.expressionLanguageFunctions(workflowDescriptor.backendDescriptor, jobKey, configDescriptor)) map { inputs =>
      val jobDescriptor = BackendJobDescriptor(workflowDescriptor.backendDescriptor, jobKey, inputs)
      val actorName = s"${jobDescriptor.descriptor.id}-BackendExecutionActor-${jobKey.tag}".replaceAll("\\$", "")
      val jobExecutionActor = context.actorOf(
        factory.jobExecutionActorProps(
          jobDescriptor,
          BackendConfigurationDescriptor(configDescriptor.backendConfig, configDescriptor.globalConfig)
        ),
        actorName
      )
      jobExecutionActor ! ExecuteJobCommand
      WorkflowExecutionDiff(Map(jobKey -> ExecutionStatus.Starting))
    }
  }

  private def processRunnableScatter(scatterKey: ScatterKey, data: WorkflowExecutionActorData): Try[WorkflowExecutionDiff] = {
    val lookup = data.hierarchicalLookup(scatterKey.scope, None) _

    scatterKey.scope.collection.evaluate(lookup, CromwellWdlFunctions) map {
      case a: WdlArray => WorkflowExecutionDiff(scatterKey.populate(a.value.size) + (scatterKey -> ExecutionStatus.Done))
      case v: WdlValue => throw new Throwable("Scatter collection must evaluate to an array")
    }
  }

  private def processRunnableCollector(collector: CollectorKey, data: WorkflowExecutionActorData): Try[WorkflowExecutionDiff] = {
    val shards = data.executionStore.findShardEntries(collector) collect { case (k: BackendJobDescriptorKey, v) if v == ExecutionStatus.Done => k }
    data.outputStore.generateCollectorOutput(collector, shards) match {
      case Failure(e) => Failure(new RuntimeException(s"Failed to collect output shards for call ${collector.tag}"))
      case Success(outputs) => self ! ScatterCollectionSucceededResponse(collector, outputs)
        Success(WorkflowExecutionDiff(Map(collector -> ExecutionStatus.Starting)))
    }
  }
}
