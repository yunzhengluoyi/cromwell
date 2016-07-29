package cromwell.backend.impl.spark

import cromwell.backend.{MemorySize, BackendWorkflowDescriptor}
import cromwell.backend.validation.RuntimeAttributesKeys._
import cromwell.core.{WorkflowId, WorkflowOptions}
import org.scalatest.{Matchers, WordSpecLike}
import spray.json.{JsBoolean, JsNumber, JsObject, JsString, JsValue}
import wdl4s.WdlExpression._
import wdl4s.expression.NoFunctions
import wdl4s.util.TryUtil
import wdl4s.{Call, WdlExpression, _}
import wdl4s.values.WdlValue

class SparkRuntimeAttributesSpec extends WordSpecLike with Matchers {

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

  val emptyWorkflowOptions = WorkflowOptions(JsObject(Map.empty[String, JsValue]))

  val staticDefaults = SparkRuntimeAttributes(1, MemorySize.parse("1 GB").get, None, "com.test.spark" , None, Some("local"), false)

  def workflowOptionsWithDefaultRA(defaults: Map[String, JsValue]) = {
    WorkflowOptions(JsObject(Map(
      "default_runtime_attributes" -> JsObject(defaults)
    )))
  }

  "SparkRuntimeAttributes" should {
    "return an instance of itself when there are no runtime attributes defined." in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { %s: "%s" }""").head
      assertSparkRuntimeAttributesSuccessfulCreation(runtimeAttributes, emptyWorkflowOptions, staticDefaults)
    }

    "return an instance of itself when tries to validate a valid Deploy Mode entry" in {
      val expectedRuntimeAttributes = staticDefaults.copy(deployMode = Option("cluster"))
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { deployMode: "cluster" %s: "%s" }""").head
      assertSparkRuntimeAttributesSuccessfulCreation(runtimeAttributes, emptyWorkflowOptions, expectedRuntimeAttributes)
    }

    "use workflow options as default if deployMode key is missing" in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { %s: "%s" }""").head
      val workflowOptions = workflowOptionsWithDefaultRA(Map(SparkRuntimeAttributes.SparkDeployMode -> JsString("cluster")))
      assertSparkRuntimeAttributesSuccessfulCreation(runtimeAttributes, workflowOptions, staticDefaults.copy(deployMode = Some("cluster")))
    }

    "throw an exception when tries to validate an invalid deployMode entry" in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { deployMode: 1 %s: "%s" }""").head
      assertSparkRuntimeAttributesFailedCreation(runtimeAttributes, "Expecting deployMode runtime attribute to be a String")
    }

    "return an instance of itself when tries to validate a valid Master entry" in {
      val expectedRuntimeAttributes = staticDefaults.copy(sparkMaster = Option("spark://master"))
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { master: "spark://master" %s: "%s"}""").head
      assertSparkRuntimeAttributesSuccessfulCreation(runtimeAttributes, emptyWorkflowOptions, expectedRuntimeAttributes)
    }

    "use workflow options as default if master key is missing" in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { %s: "%s" }""").head
      val workflowOptions = workflowOptionsWithDefaultRA(Map(SparkRuntimeAttributes.SparkMaster -> JsString("spark://master")))
      assertSparkRuntimeAttributesSuccessfulCreation(runtimeAttributes, workflowOptions, staticDefaults.copy(sparkMaster = Some("spark://master")))
    }

    "throw an exception when tries to validate an invalid master entry" in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { master: 1 %s: "%s" }""").head
      assertSparkRuntimeAttributesFailedCreation(runtimeAttributes, "Expecting master runtime attribute to be a String")
    }

    "return an instance of itself when tries to validate a valid Number of Executors entry" in {
      val expectedRuntimeAttributes = staticDefaults.copy(numberOfExecutors = Option(1))
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { numberOfExecutors: 1 %s: "%s" }""").head
      assertSparkRuntimeAttributesSuccessfulCreation(runtimeAttributes, emptyWorkflowOptions, expectedRuntimeAttributes)
    }

    "use workflow options as default if numberOfExecutors key is missing" in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { %s: "%s" }""").head
      val workflowOptions = workflowOptionsWithDefaultRA(Map(SparkRuntimeAttributes.NumberOfExecutorsKey -> JsNumber(1)))
      assertSparkRuntimeAttributesSuccessfulCreation(runtimeAttributes, workflowOptions, staticDefaults.copy(numberOfExecutors = Some(1)))
    }

    "throw an exception when tries to validate an invalid numberOfExecutors entry" in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { numberOfExecutors: "1" %s: "%s" }""").head
      assertSparkRuntimeAttributesFailedCreation(runtimeAttributes, "Expecting numberOfExecutors runtime attribute to be an Integer")
    }

    "return an instance of itself when tries to validate a valid failOnStderr entry" in {
      val expectedRuntimeAttributes = staticDefaults.copy(failOnStderr = true)
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { failOnStderr: "true" %s: "%s" }""").head
      val shouldBeIgnored = workflowOptionsWithDefaultRA(Map(FailOnStderrKey -> JsBoolean(false)))
      assertSparkRuntimeAttributesSuccessfulCreation(runtimeAttributes, shouldBeIgnored, expectedRuntimeAttributes)
    }

    "throw an exception when tries to validate an invalid failOnStderr entry" in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { failOnStderr: "yes" %s: "%s" }""").head
      assertSparkRuntimeAttributesFailedCreation(runtimeAttributes, "Expecting failOnStderr runtime attribute to be a Boolean or a String with values of 'true' or 'false'")
    }

    "use workflow options as default if failOnStdErr key is missing" in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { %s: "%s" }""").head
      val workflowOptions = workflowOptionsWithDefaultRA(Map(FailOnStderrKey -> JsBoolean(true)))
      assertSparkRuntimeAttributesSuccessfulCreation(runtimeAttributes, workflowOptions, staticDefaults.copy(failOnStderr = true))
    }

  }

  private def buildWorkflowDescriptor(wdl: WdlSource,
                                      inputs: Map[String, WdlValue] = Map.empty,
                                      options: WorkflowOptions = WorkflowOptions(JsObject(Map.empty[String, JsValue])),
                                      runtime: String = "") = {
    new BackendWorkflowDescriptor(
      WorkflowId.randomId(),
      NamespaceWithWorkflow.load(wdl.replaceAll("RUNTIME", runtime.format("appMainClass", "com.test.spark"))),
      inputs,
      options
    )
  }

  private def createRuntimeAttributes(wdlSource: WdlSource, runtimeAttributes: String = "") = {
    val workflowDescriptor = buildWorkflowDescriptor(wdlSource, runtime = runtimeAttributes)

    def createLookup(call: Call): ScopedLookupFunction = {
      val declarations = workflowDescriptor.workflowNamespace.workflow.declarations ++ call.task.declarations
      val knownInputs = workflowDescriptor.inputs
      WdlExpression.standardLookupFunction(knownInputs, declarations, NoFunctions)
    }

    workflowDescriptor.workflowNamespace.workflow.calls map {
      call =>
        val ra = call.task.runtimeAttributes.attrs mapValues { _.evaluate(createLookup(call), NoFunctions) }
        TryUtil.sequenceMap(ra, "Runtime attributes evaluation").get
    }
  }

  private def assertSparkRuntimeAttributesSuccessfulCreation(runtimeAttributes: Map[String, WdlValue], workflowOptions: WorkflowOptions, expectedRuntimeAttributes: SparkRuntimeAttributes): Unit = {
    try {
      assert(SparkRuntimeAttributes(runtimeAttributes, workflowOptions) == expectedRuntimeAttributes)
    } catch {
      case ex: RuntimeException => fail(s"Exception was not expected but received: ${ex.getMessage}")
    }
  }

  private def assertSparkRuntimeAttributesFailedCreation(runtimeAttributes: Map[String, WdlValue], exMsg: String): Unit = {
    try {
      SparkRuntimeAttributes(runtimeAttributes, emptyWorkflowOptions)
      fail("A RuntimeException was expected.")
    } catch {
      case ex: RuntimeException => assert(ex.getMessage.contains(exMsg))
    }
  }
}
