//package cromwell.engine.workflow
//
//import cromwell.backend.BackendJobDescriptorKey
//import cromwell.engine.ExecutionIndex._
//import cromwell.engine.workflow.lifecycle.WorkflowExecutionActor.{OutputCallKey, OutputEntry, OutputStore}
//import cromwell.engine.{CromwellWdlFunctions, EngineWorkflowDescriptor}
//import cromwell.util.{CromwellAggregatedException, TryUtil}
//import wdl4s._
//import wdl4s.expression.WdlStandardLibraryFunctions
//import wdl4s.values.{WdlArray, WdlCallOutputsObject, WdlValue}
//
//import scala.language.postfixOps
//import scala.util.{Failure, Success, Try}
//
///**
//   * Resolve and evaluate call input declarations
//   *
//   * As a result, the following wdl
//   *
//   * task t {
//   *   File q
//   *   String r = read_string(q) #(1)
//   *   String s
//   * }
//   *
//   * workflow w {
//   *   File f
//   *   String a = read_string(f) #(2)
//   *   call t { input:
//   *              s = a + read_string(f), #(3)
//   *              q = f
//   *          }
//   * }
//   *
//   * will be evaluated such that
//   *
//   * read_string (1) will be evaluated using BFN
//   * read_string (2) will be evaluated using EFN
//   * read_string (3) will be evaluated using BFN
//   *
//   * Where
//   *   BFN is the WDL function implementation from the backend t will run on
//   *   EFN is the WDL function implementation from the engine
//   *
//   * q, r and s will be evaluated sequentially, such that when evaluating r,
//   * the value of q has already been resolved in the appropriate scope and evaluated by the appropriate wdl functions.
//   */
//class JobInputEvaluator(workflowDescriptor: EngineWorkflowDescriptor) {
//
//  private val calls = workflowDescriptor.backendDescriptor.workflowNamespace.workflow.calls
//
//  /*
//   *  /|\ this could be very time consuming, as it potentially involves IO when using engine functions
//   * /_._\
//   */
//  def resolveAndEvaluate(jobKey: BackendJobDescriptorKey,
//                         wdlFunctions: WdlStandardLibraryFunctions,
//                         outputStore: OutputStore): Try[Map[LocallyQualifiedName, WdlValue]] = {
//    val call = jobKey.call
//    lazy val callInputsFromFile = unqualifiedInputsFromInputFile(call)
//    lazy val workflowScopedLookup = hierarchicalLookup(jobKey.call, jobKey.index, outputStore) _
//    //buildWorkflowScopedLookup(jobKey, outputStore) _
//
//    // Try to resolve, evaluate and coerce declarations in order
//    val inputEvaluationAttempt = call.task.declarations.foldLeft(Map.empty[LocallyQualifiedName, Try[WdlValue]])((inputs, declaration) => {
//      val name = declaration.name
//
//      // Try to resolve the declaration, and upon success evaluate the expression
//      // If the declaration is resolved but can't be evaluated this will throw an evaluation exception
//      // If it can't be resolved it's ignored and won't appear in the final input map
//      val evaluated: Option[Try[WdlValue]] = declaration.expression match {
//        // Static expression in the declaration
//        case Some(expr) => Option(expr.evaluate(buildMapBasedLookup(inputs), wdlFunctions))
//        // Expression found in the input mappings
//        case None if call.inputMappings.contains(name) => Option(call.inputMappings(name).evaluate(workflowScopedLookup, wdlFunctions))
//        // Expression found in the input file
//        case None if callInputsFromFile.contains(name) => Option(Success(callInputsFromFile(name)))
//        // Expression can't be found
//        case _ => None
//      }
//
//      // Leave out unresolved declarations
//      evaluated match {
//        case Some(value) =>
//          val coercedValue = value flatMap declaration.wdlType.coerceRawValue
//          inputs + ((name, coercedValue))
//        case None => inputs
//      }
//    })
//
//    TryUtil.sequenceMap(inputEvaluationAttempt, s"Input evaluation for Call ${call.fullyQualifiedName} failed")
//  }
//
//  private def splitFqn(fullyQualifiedName: FullyQualifiedName): (String, String) = {
//    val lastIndex = fullyQualifiedName.lastIndexOf(".")
//    (fullyQualifiedName.substring(0, lastIndex), fullyQualifiedName.substring(lastIndex + 1))
//  }
//
//  // Split inputs map (= evaluated workflow declarations + coerced json inputs) into [init\.*].last
//  private lazy val splitInputs = workflowDescriptor.backendDescriptor.inputs map {
//    case (fqn, v) => splitFqn(fqn) -> v
//  }
//
//  // Unqualified workflow level inputs
//  private lazy val unqualifiedWorkflowInputs: Map[LocallyQualifiedName, WdlValue] = splitInputs collect {
//    case((root, inputName), v) if root == workflowDescriptor.namespace.workflow.unqualifiedName => inputName -> v
//  }
//
//  // Unqualified call inputs for a specific call, from the input json
//  private def unqualifiedInputsFromInputFile(call: Call): Map[LocallyQualifiedName, WdlValue] = splitInputs collect {
//    case((root, inputName), v) if root == call.fullyQualifiedName => inputName -> v
//  }
//
//  def hierarchicalLookup(scope: Scope, index: ExecutionIndex, outputStore: OutputStore)(identifier: String): WdlValue = {
//    // First lookup calls
//    lookupCall(scope, index, outputStore, identifier) recoverWith {
//      // Lookup in the same scope (currently no scope support this but say we have scatter declarations, or multiple scatter variables..)
//      case _: VariableNotFoundException | _: WdlExpressionException => scopedLookup(scope, index, outputStore, identifier)
//    } recover {
//      // Lookup parent if present
//      case _: VariableNotFoundException | _: WdlExpressionException => scope.parent match {
//        case Some(parent) => hierarchicalLookup(parent, index, outputStore)(identifier)
//        case None => throw new VariableNotFoundException(s"Can't find $identifier")
//      }
//    } get
//  }
//
//  private def scopedLookup(scope: Scope, index: ExecutionIndex, outputStore: OutputStore, identifier: String): Try[WdlValue] = {
//    def scopedLookupFunction = scope match {
//      case scatter: Scatter if index.isDefined => lookupScatter(scatter, index.get, outputStore) _
//      case scatter: Scatter => throw new RuntimeException(s"Found a non indexed scope inside a scatter")
//      case workflow: Workflow => lookupWorkflowDeclaration _
//      case _ => (_: String) => Failure(new VariableNotFoundException(s"Can't find $identifier in scope $scope"))
//    }
//
//    scopedLookupFunction(identifier)
//  }
//
//  // In this case, the scopedLookup function is effectively equivalent to looking into unqualifiedWorkflowInputs for the value
//  // because the resolution / evaluation / coercion has already happened in the MaterializeWorkflowDescriptorActor
//  private def lookupWorkflowDeclaration(identifier: String) = {
//    unqualifiedWorkflowInputs.get(identifier) match {
//      case Some(value) => Success(value)
//      case None => Failure(new WdlExpressionException(s"Could not resolve variable $identifier as a workflow input"))
//    }
//  }
//
//  private def lookupScatter(scatter: Scatter, index: Int, outputStore: OutputStore)(identifier: String): Try[WdlValue] = {
//    if (identifier == scatter.item) {
//      // Scatters are not indexed yet (they can't be nested)
//      val scatterLookup = hierarchicalLookup(scatter, None, outputStore) _
//      scatter.collection.evaluate(scatterLookup, CromwellWdlFunctions) map {
//        case collection: WdlArray if collection.value.isDefinedAt(index) => collection.value(index)
//        case collection: WdlArray => throw new RuntimeException(s"Index $index out of bound in $collection for scatter ${scatter.fullyQualifiedName}")
//        case other => throw new RuntimeException(s"Scatter ${scatter.fullyQualifiedName} collection is not an array: $other")
//      } recover {
//        case e => throw new RuntimeException(s"Failed to evaluate collection for scatter ${scatter.fullyQualifiedName}", e)
//      }
//    } else {
//      Failure(new VariableNotFoundException(identifier))
//    }
//  }
//
//  private def lookupCall(scope: Scope, scopeIndex: ExecutionIndex, outputStore: OutputStore, identifier: String): Try[WdlCallOutputsObject] = {
//    calls find { _.unqualifiedName == identifier } match {
//      case Some(matchedCall) =>
//        /**
//          * After matching the Call, this determines if the `key` depends on a single shard
//          * of a scatter'd job or if it depends on the whole thing.  Right now, the heuristic
//          * is "If we're both in a scatter block together, then I depend on a shard.  If not,
//          * I depend on the collected value"
//          *
//          * TODO: nested-scatter - this will likely not be sufficient for nested scatters
//          */
//        val index: ExecutionIndex = matchedCall.closestCommonAncestor(scope) flatMap {
//          case s: Scatter => scopeIndex
//          case _ => None
//        }
//        fetchCallOutputEntries(matchedCall, index, outputStore)
//      case None => Failure(new WdlExpressionException(s"Could not find a call with identifier '$identifier'"))
//    }
//  }
//
//  def fetchCallOutputEntries(call: Call, index: ExecutionIndex, outputStore: OutputStore): Try[WdlCallOutputsObject] = {
//    def outputEntriesToMap(outputs: Traversable[OutputEntry]): Map[String, Try[WdlValue]] = {
//      outputs map { output =>
//        output.wdlValue match {
//          case Some(wdlValue) => output.name -> Success(wdlValue)
//          case None => output.name -> Failure(new RuntimeException(s"Could not retrieve output ${output.name} value"))
//        }
//      } toMap
//    }
//
//    outputStore.get(OutputCallKey(call, index)) match {
//      case Some(outputs) =>
//        TryUtil.sequenceMap(outputEntriesToMap(outputs), s"Output fetching for call ${call.unqualifiedName}") map { outputsMap =>
//          WdlCallOutputsObject(call, outputsMap)
//        }
//      case None => Failure(new RuntimeException(s"Could not find call ${call.unqualifiedName}"))
//    }
//  }
//
//  private def buildMapBasedLookup(evaluatedDeclarations: Map[LocallyQualifiedName, Try[WdlValue]])(identifier: String): WdlValue = {
//    val successfulEvaluations = evaluatedDeclarations collect {
//      case (k, v) if v.isSuccess => k -> v.get
//    }
//    successfulEvaluations.getOrElse(identifier, throw new WdlExpressionException(s"Could not resolve variable $identifier as a task input"))
//  }
//
//
//
//}