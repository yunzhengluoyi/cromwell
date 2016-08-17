package cromwell.backend.impl.spark

import akka.testkit.{EventFilter, ImplicitSender, TestDuration}
import com.typesafe.config.ConfigFactory
import cromwell.backend.BackendWorkflowInitializationActor.Initialize
import cromwell.backend.{BackendWorkflowDescriptor, BackendConfigurationDescriptor}
import cromwell.core.{TestKitSuite, WorkflowId, WorkflowOptions}
import org.scalatest.{Matchers, WordSpecLike}
import spray.json.{JsValue, JsObject}
import wdl4s._
import wdl4s.values.WdlValue
import scala.concurrent.duration._

class SparkInitializationActorSpec  extends TestKitSuite("SparkInitializationActorSpec") with WordSpecLike with Matchers  with ImplicitSender {

  val Timeout = 5.second.dilated

  val HelloWorld =
    """
      |task hello {
      |  command {
      |    helloApp
      |  }
      |  output {
      |    String salutation = read_string(stdout())
      |  }
      |
      |  RUNTIME
      |}
      |
      |workflow hello {
      |  call hello
      |}
    """.stripMargin

  val defaultBackendConfig = new BackendConfigurationDescriptor(ConfigFactory.parseString("{}"), ConfigFactory.load())

  private def buildWorkflowDescriptor(wdl: WdlSource,
                                      inputs: Map[String, WdlValue] = Map.empty,
                                      options: WorkflowOptions = WorkflowOptions(JsObject(Map.empty[String, JsValue])),
                                      runtime: String = "") = {
    new BackendWorkflowDescriptor(
      WorkflowId.randomId(),
      NamespaceWithWorkflow.load(wdl.replaceAll("RUNTIME", runtime)),
      inputs,
      options
    )
  }

  private def getSparkBackend(workflowDescriptor: BackendWorkflowDescriptor, calls: Seq[Call], conf: BackendConfigurationDescriptor) = {
    system.actorOf(SparkInitializationActor.props(workflowDescriptor, calls, conf, emptyActor))
  }

  "SparkInitializationActor" should {
    "log a warning message when there are unsupported runtime attributes" in {
      within(Timeout) {
        EventFilter.warning(message = s"Key/s [memory] is/are not supported by SparkBackend. Unsupported attributes will not be part of jobs executions.", occurrences = 1) intercept {
          val workflowDescriptor = buildWorkflowDescriptor(HelloWorld, runtime = """runtime { memory: 1 %s: "%s"}""".format("appMainClass", "test"))
          val backend = getSparkBackend(workflowDescriptor, workflowDescriptor.workflowNamespace.workflow.calls, defaultBackendConfig)
          backend ! Initialize
        }
      }
    }
  }
}
