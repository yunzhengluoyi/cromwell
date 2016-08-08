package cromwell

import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{EventFilter, TestActorRef, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import cromwell.core._
import cromwell.services.metadata.MetadataService.{GetMetadataQueryAction, GetStatus, StatusLookupResponse}
import cromwell.util.SampleWdl
import akka.pattern.ask
import cromwell.NewFandangledTestThing.TestWorkflowManagerSystem
import cromwell.engine.workflow.WorkflowManagerActor.RetrieveNewWorkflows
import cromwell.engine.workflow.workflowstore.WorkflowStoreActor.WorkflowSubmittedToStore
import cromwell.engine.workflow.workflowstore.{InMemoryWorkflowStore, WorkflowStoreActor}
import cromwell.server.{CromwellRootActor, CromwellSystem}
import NewFandangledTestThing._
import cromwell.services.metadata.MetadataQuery
import cromwell.webservice.PerRequest.RequestComplete
import cromwell.webservice.metadata.MetadataBuilderActor
import spray.http.StatusCode
import spray.json.{JsArray, JsBoolean, JsNumber, JsObject, JsString, JsValue}
import wdl4s.types.{WdlArrayType, WdlMapType, WdlStringType}
import wdl4s.values.{WdlArray, WdlBoolean, WdlFloat, WdlInteger, WdlMap, WdlString, WdlValue}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.{Await, ExecutionContext}

// FIXME JG: There should be no deps on CromwellTestKitSpec when this is done

// FIXME JG: Obviously this isn't the final name
final case class NewFandangledTestThing(twms: TestWorkflowManagerSystem = new TestWorkflowManagerSystem,
                                        config: Config = DefaultConfig,
                                        timeout: FiniteDuration = TimeoutDuration) extends TestKit(twms.actorSystem) {
  implicit val ec = twms.actorSystem.dispatcher

  val rootActor = buildCromwellRootActor(config)

  /**
    *
    * outputCheck is an Option so we don't have to do the extra work to pull outputs if they're not being checked
    */
  def testWdl(sampleWdl: SampleWdl,
              eventFilter: EventFilter = workflowSuccessFilter, // FIXME JG: I'm sure there's a discrete set of these
              runtime: String = "",
              workflowOptions: String = "{}",
              terminalState: WorkflowState = WorkflowSucceeded,
              outputCheck: Option[Map[FullyQualifiedName, WdlValue] => Boolean] = None
             ): Unit = {
    val sources = WorkflowSourceFiles(sampleWdl.wdlSource(runtime), sampleWdl.wdlJson, workflowOptions)
    eventFilter.intercept {
      within(timeout) {
        val workflowId = rootActor.underlyingActor.submitWorkflow(sources)
        verifyWorkflowState(rootActor.underlyingActor.serviceRegistryActor, workflowId, terminalState)
        outputCheck match {
          case Some(f) => verifyOutputs(workflowId, f)
          case None => ()
        }
      }
    }

    twms.shutdownActorSystem()

    ()
  }

  // FIXME JG: I'm curious if the TestActorRef will prove to be necessary
  private def buildCromwellRootActor(config: Config) = TestActorRef(new TestCromwellRootActor(config), name = "TestCromwellRootActor")

  private def verifyWorkflowState(serviceRegistryActor: ActorRef, workflowId: WorkflowId, expectedState: WorkflowState)(implicit ec: ExecutionContext): Unit = {
    // Continuously check the state of the workflow until it is in a terminal state
    awaitCond(getWorkflowState(workflowId, serviceRegistryActor).isTerminal)
    // Now that it's complete verify that we ended up in the state we're expecting
    assert(getWorkflowState(workflowId, serviceRegistryActor) == expectedState)
  }

  private def getWorkflowState(workflowId: WorkflowId, serviceRegistryActor: ActorRef)(implicit ec: ExecutionContext): WorkflowState = {
    val statusResponse = serviceRegistryActor.ask(GetStatus(workflowId))(timeout).collect {
      case StatusLookupResponse(_, state) => state
      case f => throw new RuntimeException(s"Unexpected status response for $workflowId: $f")
    }

    Await.result(statusResponse, Duration.Inf)
  }

  private def verifyOutputs(workflowId: WorkflowId, f: Map[FullyQualifiedName, WdlValue] => Boolean): Unit = {
    val outputs = getWorkflowOutputsFromMetadata(workflowId, rootActor.underlyingActor.serviceRegistryActor)
    if (!f(outputs)) throw new RuntimeException("Outputs did not conform to supplied function")
  }

  private def getWorkflowOutputsFromMetadata(id: WorkflowId, serviceRegistryActor: ActorRef): Map[FullyQualifiedName, WdlValue] = {
    getWorkflowMetadata(id, serviceRegistryActor, None).getFields(WorkflowMetadataKeys.Outputs).toList match {
      case head::_ => head.asInstanceOf[JsObject].fields.map( x => (x._1, jsValueToWdlValue(x._2)))
      case _ => Map.empty
    }
  }

  private def getWorkflowMetadata(workflowId: WorkflowId,
                                  serviceRegistryActor: ActorRef,
                                  key: Option[String] = None)(implicit ec: ExecutionContext): JsObject = {
    // MetadataBuilderActor sends its response to context.parent, so we can't just use an ask to talk to it here
    val message = GetMetadataQueryAction(MetadataQuery(workflowId, None, key, None, None))
    val parentProbe = TestProbe()

    TestActorRef(MetadataBuilderActor.props(serviceRegistryActor), parentProbe.ref, s"MetadataActor-${UUID.randomUUID()}") ! message
    val metadata = parentProbe.expectMsgPF(timeout) {
      // Because of type erasure the scala compiler can't check that the RequestComplete generic type will be (StatusCode, JsObject), which would generate a warning
      // As long as Metadata sends back a JsObject this is safe
      case response: RequestComplete[(StatusCode, JsObject)] @unchecked => response.response._2
    }

    system.stop(parentProbe.ref)
    metadata
  }
}

object NewFandangledTestThing {
  lazy val DefaultConfig = ConfigFactory.load
  val TimeoutDuration = 60 seconds

  /**
    * Loans a NewFandangledTestThing in order to run a test
    */
  // FIXME JG: Not the final name
  def withTestThing[T](block: NewFandangledTestThing => T): T = {
    val testerThinger = NewFandangledTestThing()

    try {
      block(testerThinger)
    } finally {
      if (!testerThinger.twms.actorSystem.isTerminated) testerThinger.twms.shutdownActorSystem()
    }
  }

  class TestWorkflowManagerSystem extends CromwellSystem {
    override protected def systemName: String = "test-system"
    // FIXME JG: Config
    override protected def newActorSystem() = ActorSystem(systemName, ConfigFactory.parseString(CromwellTestkitSpec.ConfigText))

    override def shutdownActorSystem() = {
      println("HEY HEY HO HO SOMEONE JUST ASKED ME TO GO") // FIXME: JG remove this when done
      super.shutdownActorSystem()
    }
  }

  class TestCromwellRootActor(config: Config) extends CromwellRootActor {
    // FIXME JG: Move SRAI into its own land
    override lazy val serviceRegistryActor = CromwellTestkitSpec.ServiceRegistryActorInstance
    override lazy val workflowStore = new InMemoryWorkflowStore

    def submitWorkflow(sources: WorkflowSourceFiles): WorkflowId = {
      val submitMessage = WorkflowStoreActor.SubmitWorkflow(sources)
      val result = Await.result(workflowStoreActor.ask(submitMessage)(TimeoutDuration), Duration.Inf).asInstanceOf[WorkflowSubmittedToStore].workflowId
      workflowManagerActor ! RetrieveNewWorkflows
      result
    }
  }

  def jsValueToWdlValue(jsValue: JsValue): WdlValue = {
    jsValue match {
      case str: JsString => WdlString(str.value)
      case JsNumber(number) if number.scale == 0 => WdlInteger(number.intValue)
      case JsNumber(number) => WdlFloat(number.doubleValue)
      case JsBoolean(bool) => WdlBoolean(bool)
      case array: JsArray =>
        val valuesArray = array.elements.map(jsValueToWdlValue)
        if (valuesArray.isEmpty) WdlArray(WdlArrayType(WdlStringType), Seq.empty)
        else WdlArray(WdlArrayType(valuesArray.head.wdlType), valuesArray)
      case map: JsObject =>
        // TODO: currently assuming all keys are String. But that's not WDL-complete...
        val valuesMap: Map[WdlValue, WdlValue] = map.fields.map { case (fieldName, fieldValue) => (WdlString(fieldName), jsValueToWdlValue(fieldValue)) }
        if (valuesMap.isEmpty) WdlMap(WdlMapType(WdlStringType, WdlStringType), Map.empty)
        else WdlMap(WdlMapType(WdlStringType, valuesMap.head._2.wdlType), valuesMap)
    }
  }

  def workflowSuccessFilter = EventFilter.info(pattern = "transition from FinalizingWorkflowState to WorkflowSucceededState", occurrences = 1)
  def workflowFailureFilter = EventFilter.info(pattern = "transitioning from FinalizingWorkflowState to WorkflowFailedState", occurrences = 1)
}
