package cromwell.backend.impl.spark

import java.io.{File, FileWriter, Writer}
import java.nio.file.{Path, Paths}

import akka.testkit.{ImplicitSender, TestActorRef}
import com.typesafe.config.ConfigFactory
import cromwell.backend.{BackendSpec, BackendConfigurationDescriptor, BackendJobDescriptor}
import cromwell.backend.BackendJobExecutionActor.{FailedNonRetryableResponse, SucceededResponse}
import better.files._
import cromwell.backend.io.{JobPaths}
import cromwell.core.{TestKitSuite, PathWriter, TailedWriter, UntailedWriter}
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}
import wdl4s._
import wdl4s.values.{WdlValue}

import scala.concurrent.duration._
import scala.sys.process.{Process, ProcessLogger}

class SparkJobExecutionActorSpec extends TestKitSuite("SparkJobExecutionActor")
  with WordSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfter
  with ImplicitSender {

  import BackendSpec._

  private val sparkProcess: SparkProcess = mock[SparkProcess]
  private val sparkCommands: SparkCommands = new SparkCommands

  private val helloWorldWdl =
    """
      |task hello {
      |
      |  command {
      |    sparkApp
      |  }
      |  output {
      |    String salutation = read_string(stdout())
      |  }
      |  RUNTIME
      |}
      |
      |workflow hello {
      |  call hello
      |}
    """.stripMargin

  private val failedOnStderr =
    """
      |runtime {
      | appMainClass: "test"
      | failOnStderr: true
      |}
    """.stripMargin

  private val passOnStderr =
    """
      |runtime {
      | appMainClass: "test"
      | failOnStderr: false
      |}
    """.stripMargin

  private val backendConfig = ConfigFactory.parseString(
    s"""{
        |  root = "local-cromwell-executions"
        |  filesystems {
        |    local {
        |      localization = [
        |        "hard-link", "soft-link", "copy"
        |      ]
        |    }
        |  }
        |}
        """.stripMargin)

  private val timeout = Timeout(1.seconds)

  after {
    Mockito.reset(sparkProcess)
  }

  override def afterAll(): Unit = system.shutdown()

  "executeTask method" should {
    "return succeeded task status with stdout" in {
      val jobDescriptor = prepareJob()
      val (job, jobPaths, backendConfigDesc) = (jobDescriptor.jobDescriptor, jobDescriptor.jobPaths, jobDescriptor.backendConfigurationDescriptor)
      val stubProcess = mock[Process]
      val stubUntailed = new UntailedWriter(jobPaths.stdout) with MockPathWriter
      val stubTailed = new TailedWriter(jobPaths.stderr, 100) with MockPathWriter
      val stderrResult = ""

      when(sparkProcess.commandList(any[String])).thenReturn(Seq.empty[String])
      when(sparkProcess.externalProcess(any[Seq[String]], any[ProcessLogger])).thenReturn(stubProcess)
      when(stubProcess.exitValue()).thenReturn(0)
      when(sparkProcess.tailedWriter(any[Int], any[Path])).thenReturn(stubTailed)
      when(sparkProcess.untailedWriter(any[Path])).thenReturn(stubUntailed)
      when(sparkProcess.processStderr).thenReturn(stderrResult)

      val backend = TestActorRef(new SparkJobExecutionActor(job, backendConfigDesc) {
        override lazy val extProcess = sparkProcess
        override lazy val cmds = sparkCommands
      }).underlyingActor

      whenReady(backend.execute, timeout) { response =>
        response shouldBe a[SucceededResponse]
        verify(sparkProcess, times(1)).externalProcess(any[Seq[String]], any[ProcessLogger])
        verify(sparkProcess, times(1)).tailedWriter(any[Int], any[Path])
        verify(sparkProcess, times(1)).untailedWriter(any[Path])
      }

      cleanUpJob(jobPaths)
    }

    "return failed task status on non-zero process exit" in {
      val jobDescriptor = prepareJob()
      val (job, jobPaths, backendConfigDesc) = (jobDescriptor.jobDescriptor, jobDescriptor.jobPaths, jobDescriptor.backendConfigurationDescriptor)

      val backend = TestActorRef(new SparkJobExecutionActor(job, backendConfigDesc) {
        override lazy val cmds = sparkCommands
        override lazy val extProcess = sparkProcess
      }).underlyingActor
      val stubProcess = mock[Process]
      val stubUntailed = new UntailedWriter(jobPaths.stdout) with MockPathWriter
      val stubTailed = new TailedWriter(jobPaths.stderr, 100) with MockPathWriter
      val stderrResult = ""

      when(sparkProcess.externalProcess(any[Seq[String]], any[ProcessLogger])).thenReturn(stubProcess)
      when(stubProcess.exitValue()).thenReturn(-1)
      when(sparkProcess.tailedWriter(any[Int], any[Path])).thenReturn(stubTailed)
      when(sparkProcess.untailedWriter(any[Path])).thenReturn(stubUntailed)
      when(sparkProcess.processStderr).thenReturn(stderrResult)

      whenReady(backend.execute, timeout) { response =>
        response shouldBe a[FailedNonRetryableResponse]
        assert(response.asInstanceOf[FailedNonRetryableResponse].throwable.getMessage.contains(s"Execution process failed. Spark returned non zero status code:"))
      }

      cleanUpJob(jobPaths)
    }

    "return failed task status when stderr is non-empty but process exit with zero return code" in {
      val jobDescriptor = prepareJob(runtimeString = failedOnStderr)
      val (job, jobPaths, backendConfigDesc) = (jobDescriptor.jobDescriptor, jobDescriptor.jobPaths, jobDescriptor.backendConfigurationDescriptor)

      val backend = TestActorRef(new SparkJobExecutionActor(job, backendConfigDesc) {
        override lazy val cmds = sparkCommands
        override lazy val extProcess = sparkProcess
      }).underlyingActor
      val stubProcess = mock[Process]
      val stubUntailed = new UntailedWriter(jobPaths.stdout) with MockPathWriter
      val stubTailed = new TailedWriter(jobPaths.stderr, 100) with MockPathWriter
      jobPaths.stderr < "failed"

      when(sparkProcess.externalProcess(any[Seq[String]], any[ProcessLogger])).thenReturn(stubProcess)
      when(stubProcess.exitValue()).thenReturn(0)
      when(sparkProcess.tailedWriter(any[Int], any[Path])).thenReturn(stubTailed)
      when(sparkProcess.untailedWriter(any[Path])).thenReturn(stubUntailed)

      whenReady(backend.execute, timeout) { response =>
        response shouldBe a[FailedNonRetryableResponse]
        assert(response.asInstanceOf[FailedNonRetryableResponse].throwable.getMessage.contains(s"Execution process failed although return code is zero but stderr is not empty"))
      }

      cleanUpJob(jobPaths)
    }

    "return succeeded task status when stderr is non-empty but process exit with zero return code" in {
      val jobDescriptor = prepareJob()
      val (job, jobPaths, backendConfigDesc) = (jobDescriptor.jobDescriptor, jobDescriptor.jobPaths, jobDescriptor.backendConfigurationDescriptor)

      val backend = TestActorRef(new SparkJobExecutionActor(job, backendConfigDesc) {
        override lazy val cmds = sparkCommands
        override lazy val extProcess = sparkProcess
      }).underlyingActor
      val stubProcess = mock[Process]
      val stubUntailed = new UntailedWriter(jobPaths.stdout) with MockPathWriter
      val stubTailed = new TailedWriter(jobPaths.stderr, 100) with MockPathWriter

      when(sparkProcess.externalProcess(any[Seq[String]], any[ProcessLogger])).thenReturn(stubProcess)
      when(stubProcess.exitValue()).thenReturn(0)
      when(sparkProcess.tailedWriter(any[Int], any[Path])).thenReturn(stubTailed)
      when(sparkProcess.untailedWriter(any[Path])).thenReturn(stubUntailed)

      whenReady(backend.execute, timeout) { response =>
        response shouldBe a[SucceededResponse]
        verify(sparkProcess, times(1)).externalProcess(any[Seq[String]], any[ProcessLogger])
        verify(sparkProcess, times(1)).tailedWriter(any[Int], any[Path])
        verify(sparkProcess, times(1)).untailedWriter(any[Path])
      }

      cleanUpJob(jobPaths)
    }

  }

  private def write(file: File, contents: String) = {
    val writer = new FileWriter(file)
    writer.write(contents)
    writer.flush()
    writer.close()
    file
  }

  private def cleanUpJob(jobPaths: JobPaths): Unit = jobPaths.workflowRoot.delete(true)

  private def prepareJob(wdlSource: WdlSource = helloWorldWdl, runtimeString: String = passOnStderr, inputFiles: Option[Map[String, WdlValue]] = None): TestJobDescriptor = {
    val backendWorkflowDescriptor = buildWorkflowDescriptor(wdl = wdlSource, inputs = inputFiles.getOrElse(Map.empty), runtime = runtimeString)
    val backendConfigurationDescriptor = BackendConfigurationDescriptor(backendConfig, ConfigFactory.load)
    val jobDesc = jobDescriptorFromSingleCallWorkflow(backendWorkflowDescriptor, inputFiles.getOrElse(Map.empty))
    val jobPaths = new JobPaths(backendWorkflowDescriptor, backendConfig, jobDesc.key)
    val executionDir = jobPaths.callRoot
    val stdout = Paths.get(executionDir.path.toString, "stdout")
    stdout.toString.toFile.createIfNotExists(false)
    val stderr = Paths.get(executionDir.path.toString, "stderr")
    stderr.toString.toFile.createIfNotExists(false)
    TestJobDescriptor(jobDesc, jobPaths, backendConfigurationDescriptor)
  }

  private case class TestJobDescriptor(jobDescriptor: BackendJobDescriptor, jobPaths: JobPaths, backendConfigurationDescriptor: BackendConfigurationDescriptor)

  trait MockWriter extends Writer {
    var closed = false

    override def close() = closed = true

    override def flush() = {}

    override def write(a: Array[Char], b: Int, c: Int) = {}
  }

  trait MockPathWriter extends PathWriter {
    override lazy val writer: Writer = new MockWriter {}
    override val path: Path = mock[Path]
  }
}
