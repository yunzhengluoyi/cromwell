package cromwell.engine.workflow.lifecycle

import cromwell.backend.{BackendJobDescriptorKey, JobKey}
import cromwell.core._
import cromwell.engine.ExecutionIndex._
import cromwell.engine.ExecutionStatus._
import cromwell.engine.workflow.lifecycle.WorkflowExecutionActor.CollectorKey
import cromwell.engine.workflow.lifecycle.WorkflowExecutionActorData._
import cromwell.engine.{CromwellWdlFunctions, EngineWorkflowDescriptor, ExecutionStatus}
import cromwell.util.TryUtil
import cromwell.webservice.WdlValueJsonFormatter
import wdl4s._
import wdl4s.expression.WdlStandardLibraryFunctions
import wdl4s.types.{WdlArrayType, WdlType}
import wdl4s.values.{WdlArray, WdlCallOutputsObject, WdlValue}

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object WorkflowExecutionActorData {
  type ExecutionStore = Map[JobKey, ExecutionStatus]
  type ExecutionStoreEntry = (JobKey, ExecutionStatus)

  case class OutputEntry(name: String, wdlType: WdlType, wdlValue: Option[WdlValue])
  case class OutputCallKey(call: Scope, index: ExecutionIndex)
  type OutputStore = Map[OutputCallKey, Traversable[OutputEntry]]
}

object WorkflowExecutionDiff {
  def empty = WorkflowExecutionDiff(Map.empty)
}
/** Data differential between current execution data, and updates performed in a method that needs to be merged. */
final case class WorkflowExecutionDiff(executionStore: ExecutionStore) {
  def containsNewEntry = executionStore.exists(_._2 == NotStarted)
}

case class WorkflowExecutionActorData(workflowDescriptor: EngineWorkflowDescriptor,
                                      executionStore: ExecutionStore,
                                      outputStore: OutputStore) {

  /*
  *  /|\ this could be very time consuming, as it potentially involves IO when using engine functions
  * /_._\
  */
  def resolveAndEvaluate(jobKey: BackendJobDescriptorKey,
                         wdlFunctions: WdlStandardLibraryFunctions): Try[Map[LocallyQualifiedName, WdlValue]] = {
    val call = jobKey.call
    lazy val callInputsFromFile = unqualifiedInputsFromInputFile(call)
    lazy val workflowScopedLookup = hierarchicalLookup(jobKey.call, jobKey.index) _

    // Try to resolve, evaluate and coerce declarations in order
    val inputEvaluationAttempt = call.task.declarations.foldLeft(Map.empty[LocallyQualifiedName, Try[WdlValue]])((inputs, declaration) => {
      val name = declaration.name

      // Try to resolve the declaration, and upon success evaluate the expression
      // If the declaration is resolved but can't be evaluated this will throw an evaluation exception
      // If it can't be resolved it's ignored and won't appear in the final input map
      val evaluated: Option[Try[WdlValue]] = declaration.expression match {
        // Static expression in the declaration
        case Some(expr) => Option(expr.evaluate(buildMapBasedLookup(inputs), wdlFunctions))
        // Expression found in the input mappings
        case None if call.inputMappings.contains(name) => Option(call.inputMappings(name).evaluate(workflowScopedLookup, wdlFunctions))
        // Expression found in the input file
        case None if callInputsFromFile.contains(name) => Option(Success(callInputsFromFile(name)))
        // Expression can't be found
        case _ => None
      }

      // Leave out unresolved declarations
      evaluated match {
        case Some(value) =>
          val coercedValue = value flatMap declaration.wdlType.coerceRawValue
          inputs + ((name, coercedValue))
        case None => inputs
      }
    })

    TryUtil.sequenceMap(inputEvaluationAttempt, s"Input evaluation for Call ${call.fullyQualifiedName} failed")
  }

  private def splitFqn(fullyQualifiedName: FullyQualifiedName): (String, String) = {
    val lastIndex = fullyQualifiedName.lastIndexOf(".")
    (fullyQualifiedName.substring(0, lastIndex), fullyQualifiedName.substring(lastIndex + 1))
  }

  // Split inputs map (= evaluated workflow declarations + coerced json inputs) into [init\.*].last
  private lazy val splitInputs = workflowDescriptor.backendDescriptor.inputs map {
    case (fqn, v) => splitFqn(fqn) -> v
  }

  // Unqualified workflow level inputs
  private lazy val unqualifiedWorkflowInputs: Map[LocallyQualifiedName, WdlValue] = splitInputs collect {
    case((root, inputName), v) if root == workflowDescriptor.namespace.workflow.unqualifiedName => inputName -> v
  }

  // Unqualified call inputs for a specific call, from the input json
  private def unqualifiedInputsFromInputFile(call: Call): Map[LocallyQualifiedName, WdlValue] = splitInputs collect {
    case((root, inputName), v) if root == call.fullyQualifiedName => inputName -> v
  }

  private def buildMapBasedLookup(evaluatedDeclarations: Map[LocallyQualifiedName, Try[WdlValue]])(identifier: String): WdlValue = {
    val successfulEvaluations = evaluatedDeclarations collect {
      case (k, v) if v.isSuccess => k -> v.get
    }
    successfulEvaluations.getOrElse(identifier, throw new WdlExpressionException(s"Could not resolve variable $identifier as a task input"))
  }

  def jobExecutionSuccess(jobKey: JobKey,
                          outputs: CallOutputs) = this.copy(executionStore = executionStore + (jobKey -> Done),
    outputStore = outputStore ++ updateSymbolStoreEntry(jobKey, outputs))

  /** Add the outputs for the specified `JobKey` to the symbol cache. */
  private def updateSymbolStoreEntry(jobKey: JobKey, outputs: CallOutputs) = {
    val newOutputEntries = outputs map {
      case (name, value) => OutputEntry(name, value.wdlValue.wdlType, Option(value.wdlValue))
    }

    outputStore + (OutputCallKey(jobKey.scope, jobKey.index) -> newOutputEntries)
  }

  /** Checks if the workflow is completed by scanning through the executionStore */
  def isWorkflowComplete: Boolean = {
    def isDone(executionStatus: ExecutionStatus): Boolean = executionStatus == ExecutionStatus.Done
    executionStore.values.forall(isDone)
  }

  def hasFailedJob: Boolean = {
    executionStore.values.exists(_ == ExecutionStatus.Failed)
  }

  def mergeExecutionDiff(diff: WorkflowExecutionDiff): WorkflowExecutionActorData = {
    this.copy(executionStore = executionStore ++ diff.executionStore)
  }

  def mergeExecutionDiffs(diffs: Traversable[WorkflowExecutionDiff]): WorkflowExecutionActorData = {
    diffs.foldLeft(this)((data, diff) => data.mergeExecutionDiff(diff))
  }

  /**
    * Attempt to start all runnable jobs and return updated state data.  This will create a new copy
    * of the state data including new pending persists.
    */
  def runnableScopes: Iterable[JobKey] = executionStore.filter(isRunnable).keys

  private def isRunnable(entry: ExecutionStoreEntry) = {
    entry match {
      case (key, ExecutionStatus.NotStarted) => arePrerequisitesDone(key)
      case _ => false
    }
  }

  def findShardEntries(key: CollectorKey): Iterable[ExecutionStoreEntry] = executionStore collect {
    case (k: BackendJobDescriptorKey, v) if k.scope == key.scope && k.isShard => (k, v)
  }

  private def arePrerequisitesDone(key: JobKey): Boolean = {
    def isDone(e: JobKey): Boolean = executionStore exists { case (k, s) => k.scope == e.scope && k.index == e.index && s == ExecutionStatus.Done }

    val upstream = key.scope.prerequisiteScopes.map(s => upstreamEntries(key, s))
    val downstream = key match {
      case collector: CollectorKey => findShardEntries(collector)
      case _ => Nil
    }
    val dependencies = upstream.flatten ++ downstream
    val dependenciesResolved = executionStore.filter(dependencies).keys forall isDone

    /**
      * We need to make sure that all prerequisiteScopes have been resolved to some entry before going forward.
      * If a scope cannot be resolved it may be because it is in a scatter that has not been populated yet,
      * therefore there is no entry in the executionStore for this scope.
      * If that's the case this prerequisiteScope has not been run yet, hence the (upstream forall {_.nonEmpty})
      */
    (upstream forall { _.nonEmpty }) && dependenciesResolved
  }

  private def upstreamEntries(entry: JobKey, prerequisiteScope: Scope): Seq[ExecutionStoreEntry] = {
    prerequisiteScope.closestCommonAncestor(entry.scope) match {
      /**
        * If this entry refers to a Scope which has a common ancestor with prerequisiteScope
        * and that common ancestor is a Scatter block, then find the shard with the same index
        * as 'entry'.  In other words, if you're in the same scatter block as your pre-requisite
        * scope, then depend on the shard (with same index).
        *
        * NOTE: this algorithm was designed for ONE-LEVEL of scattering and probably does not
        * work as-is for nested scatter blocks
        */
      case Some(ancestor: Scatter) =>
        executionStore filter { case (k, _) => k.scope == prerequisiteScope && k.index == entry.index } toSeq

      /**
        * Otherwise, simply refer to the entry the collector entry.  This means that 'entry' depends
        * on every shard of the pre-requisite scope to finish.
        */
      case _ =>
        executionStore filter { case (k, _) => k.scope == prerequisiteScope && k.index.isEmpty } toSeq
    }
  }

  /**
    * Try to generate output for a collector call, by collecting outputs for all of its shards.
    * It's fail-fast on shard output retrieval
    */
  def generateCollectorOutput(collector: CollectorKey): Try[CallOutputs] = Try {
    val shards = findShardEntries(collector) collect { case (k: BackendJobDescriptorKey, v) if v == ExecutionStatus.Done => k }

    val shardsOutputs = shards.toSeq sortBy { _.index.fromIndex } map { e =>
      fetchCallOutputEntries(e.scope, e.index) map { _.outputs } getOrElse(throw new RuntimeException(s"Could not retrieve output for shard ${e.scope} #${e.index}"))
    }
    collector.scope.task.outputs map { taskOutput =>
      val wdlValues = shardsOutputs.map(s => s.getOrElse(taskOutput.name, throw new RuntimeException(s"Could not retrieve output ${taskOutput.name}")))
      val arrayOfValues = new WdlArray(WdlArrayType(taskOutput.wdlType), wdlValues)
      taskOutput.name -> CallOutput(arrayOfValues, None)
    } toMap
  }

  private def fetchCallOutputEntries(call: Call, index: ExecutionIndex): Try[WdlCallOutputsObject] = {
    def outputEntriesToMap(outputs: Traversable[OutputEntry]): Map[String, Try[WdlValue]] = {
      outputs map { output =>
        output.wdlValue match {
          case Some(wdlValue) => output.name -> Success(wdlValue)
          case None => output.name -> Failure(new RuntimeException(s"Could not retrieve output ${output.name} value"))
        }
      } toMap
    }

    outputStore.get(OutputCallKey(call, index)) match {
      case Some(outputs) =>
        TryUtil.sequenceMap(outputEntriesToMap(outputs), s"Output fetching for call ${call.unqualifiedName}") map { outputsMap =>
          WdlCallOutputsObject(call, outputsMap)
        }
      case None => Failure(new RuntimeException(s"Could not find call ${call.unqualifiedName}"))
    }
  }

  /**
    * Lookup an identifier by
    * first looking at the completed calls map
    * and if not found traversing up the scope hierarchy from the scope from which the lookup originated.
    */
  def hierarchicalLookup(scope: Scope, index: ExecutionIndex)(identifier: String): WdlValue = {
    // First lookup calls
    lookupCall(scope, index, identifier) recoverWith {
      // Lookup in the same scope (currently no scope support this but say we have scatter declarations, or multiple scatter variables, or nested workflows..)
      case _: VariableNotFoundException | _: WdlExpressionException => scopedLookup(scope, index, identifier)
    } recover {
      // Lookup parent if present
      case _: VariableNotFoundException | _: WdlExpressionException => scope.parent match {
        case Some(parent) => hierarchicalLookup(parent, index)(identifier)
        case None => throw new VariableNotFoundException(s"Can't find $identifier")
      }
    } get
  }

  private def scopedLookup(scope: Scope, index: ExecutionIndex, identifier: String): Try[WdlValue] = {
    def scopedLookupFunction = scope match {
      case scatter: Scatter if index.isDefined => lookupScatter(scatter, index.get) _
      case scatter: Scatter => throw new RuntimeException(s"Found a non indexed scope inside a scatter")
      case workflow: Workflow => lookupWorkflowDeclaration _
      case _ => (_: String) => Failure(new VariableNotFoundException(s"Can't find $identifier in scope $scope"))
    }

    scopedLookupFunction(identifier)
  }

  // In this case, the scopedLookup function is effectively equivalent to looking into unqualifiedWorkflowInputs for the value
  // because the resolution / evaluation / coercion has already happened in the MaterializeWorkflowDescriptorActor
  private def lookupWorkflowDeclaration(identifier: String) = {
    unqualifiedWorkflowInputs.get(identifier) match {
      case Some(value) => Success(value)
      case None => Failure(new WdlExpressionException(s"Could not resolve variable $identifier as a workflow input"))
    }
  }

  private def lookupScatter(scatter: Scatter, index: Int)(identifier: String): Try[WdlValue] = {
    if (identifier == scatter.item) {
      // Scatters are not indexed yet (they can't be nested)
      val scatterLookup = hierarchicalLookup(scatter, None) _
      scatter.collection.evaluate(scatterLookup, CromwellWdlFunctions) map {
        case collection: WdlArray if collection.value.isDefinedAt(index) => collection.value(index)
        case collection: WdlArray => throw new RuntimeException(s"Index $index out of bound in $collection for scatter ${scatter.fullyQualifiedName}")
        case other => throw new RuntimeException(s"Scatter ${scatter.fullyQualifiedName} collection is not an array: $other")
      } recover {
        case e => throw new RuntimeException(s"Failed to evaluate collection for scatter ${scatter.fullyQualifiedName}", e)
      }
    } else {
      Failure(new VariableNotFoundException(identifier))
    }
  }

  private def lookupCall(scope: Scope, scopeIndex: ExecutionIndex, identifier: String): Try[WdlCallOutputsObject] = {
    val calls = executionStore.keys.view map { _.scope } collect { case c: Call => c }

    calls find { _.unqualifiedName == identifier } match {
      case Some(matchedCall) =>
        /**
          * After matching the Call, this determines if the `key` depends on a single shard
          * of a scatter'd job or if it depends on the whole thing.  Right now, the heuristic
          * is "If we're both in a scatter block together, then I depend on a shard.  If not,
          * I depend on the collected value"
          *
          * TODO: nested-scatter - this will likely not be sufficient for nested scatters
          */
        val index: ExecutionIndex = matchedCall.closestCommonAncestor(scope) flatMap {
          case s: Scatter => scopeIndex
          case _ => None
        }
        fetchCallOutputEntries(matchedCall, index)
      case None => Failure(new WdlExpressionException(s"Could not find a call with identifier '$identifier'"))
    }
  }

  def outputsJson(): String = {
    // Printing the final outputs, temporarily here until SingleWorkflowManagerActor is made in-sync with the shadow mode
    import WdlValueJsonFormatter._
    import spray.json._
    val workflowOutputs = outputStore collect {
      case (key, outputs) if key.index.isEmpty => outputs map { output =>
        s"${key.call.fullyQualifiedName}.${output.name}" -> (output.wdlValue map { _.valueString } getOrElse "N/A")
      }
    }

    "Workflow complete. Final Outputs: \n" + workflowOutputs.flatten.toMap.toJson.prettyPrint
  }

}
