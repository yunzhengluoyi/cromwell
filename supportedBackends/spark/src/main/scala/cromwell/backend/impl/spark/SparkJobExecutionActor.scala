package cromwell.backend.impl.spark

import java.nio.file.FileSystems
import java.nio.file.attribute.PosixFilePermission

import akka.actor.{Props}
import cromwell.backend.BackendJobExecutionActor.{BackendJobExecutionResponse, FailedNonRetryableResponse, SucceededResponse}
import cromwell.backend.io.JobPaths
import cromwell.backend.sfs.{SharedFileSystem, SharedFileSystemExpressionFunctions}
import cromwell.backend.{BackendConfigurationDescriptor, BackendJobDescriptor, BackendJobExecutionActor}
import wdl4s.parser.MemoryUnit
import wdl4s.util.TryUtil

import scala.concurrent.{Future, Promise}
import scala.sys.process.ProcessLogger
import scala.util.{Failure, Success, Try}
import scala.language.postfixOps

object SparkJobExecutionActor {
  val fileSystems = List(FileSystems.getDefault)

  def props(jobDescriptor: BackendJobDescriptor, configurationDescriptor: BackendConfigurationDescriptor): Props =
    Props(new SparkJobExecutionActor(jobDescriptor, configurationDescriptor))
}

class SparkJobExecutionActor(override val jobDescriptor: BackendJobDescriptor,
                             override val configurationDescriptor: BackendConfigurationDescriptor) extends BackendJobExecutionActor with SharedFileSystem {

  import SparkJobExecutionActor._
  import better.files._
  import cromwell.core.PathFactory._

  private val tag = s"SparkJobExecutionActor-${jobDescriptor.call.fullyQualifiedName}:"

  lazy val cmds = new SparkCommands
  lazy val extProcess = new SparkProcess
  lazy val clusterManagerConfig = configurationDescriptor.backendConfig.getConfig("cluster-manager")
  private val fileSystemsConfig = configurationDescriptor.backendConfig.getConfig("filesystems")
  override val sharedFileSystemConfig = fileSystemsConfig.getConfig("local")
  private val workflowDescriptor = jobDescriptor.descriptor
  private val jobPaths = new JobPaths(workflowDescriptor, configurationDescriptor.backendConfig, jobDescriptor.key)

  // Files
  private val executionDir = jobPaths.callRoot
  private val scriptPath = jobPaths.script

  private lazy val stdoutWriter = extProcess.untailedWriter(jobPaths.stdout)
  private lazy val stderrWriter = extProcess.tailedWriter(100, jobPaths.stderr)

  private val call = jobDescriptor.key.call
  private val callEngineFunction = SharedFileSystemExpressionFunctions(jobPaths, fileSystems)

  private val lookup = jobDescriptor.inputs.apply _

  private val executionResponse = Promise[BackendJobExecutionResponse]()

  private val runtimeAttributes = {
    val evaluateAttrs = call.task.runtimeAttributes.attrs mapValues (_.evaluate(lookup, callEngineFunction))
    // Fail the call if runtime attributes can't be evaluated
    val runtimeMap = TryUtil.sequenceMap(evaluateAttrs, "Runtime attributes evaluation").get
    SparkRuntimeAttributes(runtimeMap, jobDescriptor.descriptor.workflowOptions)
  }

  /**
    * Restart or resume a previously-started job.
    */
  override def recover: Future[BackendJobExecutionResponse] = {
    log.warning("{} HtCondor backend currently doesn't support recovering jobs. Starting {} again.", tag, jobDescriptor.key.call.fullyQualifiedName)
    Future(executeTask())
  }

  /**
    * Execute a new job.
    */
  override def execute: Future[BackendJobExecutionResponse] = {
    prepareAndExecute
    executionResponse.future
  }

  private def executeTask(): BackendJobExecutionResponse = {
    val argv = Seq(SparkCommands.SHELL_CMD, scriptPath.toString)
    val process = extProcess.externalProcess(argv, ProcessLogger(stdoutWriter.writeWithNewline, stderrWriter.writeWithNewline))
    val jobReturnCode = Try(process.exitValue()) // blocks until process (i.e. spark submission) finishes
    log.debug("{} Return code of spark submit command: {}", tag, jobReturnCode)
    List(stdoutWriter.writer, stderrWriter.writer).foreach(_.flushAndClose())
    (jobReturnCode, runtimeAttributes.failOnStderr) match {
      case (Success(0), false) => processSuccess(0)
      case (Success(0), true) if jobPaths.stderr.lines.toList.isEmpty => processSuccess(0)
      case (Success(0), true) => FailedNonRetryableResponse(jobDescriptor.key,
        new IllegalStateException(s"Execution process failed although return code is zero but stderr is not empty"), Option(0))
      case (Success(rc), _) => FailedNonRetryableResponse(jobDescriptor.key,
        new IllegalStateException(s"Execution process failed. Spark returned non zero status code: $jobReturnCode"), Option(rc))
      case (Failure(error), _) => FailedNonRetryableResponse(jobDescriptor.key, error, None)
    }
  }

  private def processSuccess(rc: Int) = {
    evaluateOutputs(callEngineFunction, outputMapper(jobPaths)) match {
      case Success(outputs) => SucceededResponse(jobDescriptor.key, Some(rc), outputs)
      case Failure(e) =>
        val message = Option(e.getMessage) map {
          ": " + _
        } getOrElse ""
        FailedNonRetryableResponse(jobDescriptor.key, new Throwable("Failed post processing of outputs" + message, e), Option(rc))
    }
  }

  /**
    * Abort a running job.
    */
  override def abort(): Unit = Future.failed(new UnsupportedOperationException("SparkBackend currently doesn't support aborting jobs."))


  private def createExecutionFolderAndScript(): Unit = {
    try {
      log.debug("{} Creating execution folder: {}", tag, executionDir)
      executionDir.toString.toFile.createIfNotExists(true)

      log.debug("{} Resolving job command", tag)
      val command = localizeInputs(jobPaths.callRoot, docker = false, fileSystems, jobDescriptor.inputs) flatMap {
        localizedInputs => call.task.instantiateCommand(localizedInputs, callEngineFunction, identity)
      }

      log.debug("{} Creating bash script for executing command: {}", tag, command)
      //TODO: we should use shapeless Heterogeneous list here not good to have generic map
      val attributes: Map[String, Any] = Map(
        SparkCommands.APP_MAIN_CLASS -> runtimeAttributes.appMainClass,
        SparkCommands.DEPLOY_MODE -> runtimeAttributes.deployMode,
        SparkCommands.MASTER -> runtimeAttributes.sparkMaster.get,
        SparkCommands.EXECUTOR_CORES -> runtimeAttributes.executorCores,
        SparkCommands.EXECUTOR_MEMORY -> runtimeAttributes.executorMemory.to(MemoryUnit.GB).amount.toLong,
        SparkCommands.SPARK_APP_WITH_ARGS -> command.get
      )

      cmds.writeScript(cmds.sparkSubmitCommand(attributes), scriptPath, executionDir) // Writes the bash script for executing the command
      scriptPath.addPermission(PosixFilePermission.OWNER_EXECUTE) // Add executable permissions to the script.
    } catch {
      case ex: Exception =>
        log.error(ex, "Failed to prepare task: " + ex.getMessage)
        throw ex
    }
  }

  private def prepareAndExecute: Unit = {
    Try {
      createExecutionFolderAndScript()
      executionResponse success executeTask()
    } recover {
      case exception => executionResponse success  FailedNonRetryableResponse(jobDescriptor.key, exception, None)
    }
  }

}