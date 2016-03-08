package cromwell.engine.backend

import cromwell.engine.backend.runtimeattributes.{CromwellRuntimeAttributes, ContinueOnReturnCodeFlag}
import cromwell.engine.{ExecutionHash, WorkflowDescriptor}
import cromwell.engine.workflow.{BackendCallKey, CallKey, FinalCallKey}
import cromwell.webservice.WorkflowMetadataResponse
import wdl4s._
import wdl4s.values.WdlValue

import scala.concurrent.{Future, ExecutionContext}


/** Aspires to be an equivalent of `TaskDescriptor` in the pluggable backends world, describing a job in a way
  * that is complete enough for it to be executed on any backend and free of references to engine types.
  * Currently a ways to go in freedom from engine types.
  *
  * @tparam K CallKey subtype
  */
sealed trait JobDescriptor[K <: CallKey] {
  def workflowDescriptor: WorkflowDescriptor
  def key: K
  def locallyQualifiedInputs: CallInputs
}

final case class BackendCallJobDescriptor(workflowDescriptor: WorkflowDescriptor,
                                          key: BackendCallKey,
                                          locallyQualifiedInputs: CallInputs = Map.empty) extends JobDescriptor[BackendCallKey] {

  // PBE temporarily still required.  Once we have call-scoped Backend actors they will know themselves and the
  // backend won't need to be in the WorkflowDescriptor and this method won't need to exist.
  def backend = workflowDescriptor.backend

  def callRootPath = backend.callRootPath(this)

  def callRootPathWithBaseRoot(baseRoot: String) = backend.callRootPathWithBaseRoot(this, baseRoot)

  @throws[IllegalArgumentException]
  lazy val runtimeAttributes = CromwellRuntimeAttributes(key.scope.task.runtimeAttributes, this, Option(workflowDescriptor.workflowOptions))

  /** Given the specified value for the Docker hash, return the overall hash for this `BackendCall`. */
  private def hashGivenDockerHash(dockerHash: Option[String]): ExecutionHash = {
    val orderedInputs = locallyQualifiedInputs.toSeq.sortBy(_._1)
    val orderedOutputs = key.scope.task.outputs.sortWith((l, r) => l.name > r.name)
    val orderedRuntime = Seq(
      ("docker", dockerHash getOrElse ""),
      ("zones", runtimeAttributes.zones.sorted.mkString(",")),
      ("failOnStderr", runtimeAttributes.failOnStderr.toString),
      ("continueOnReturnCode", runtimeAttributes.continueOnReturnCode match {
        case ContinueOnReturnCodeFlag(bool) => bool.toString
        case ContinueOnReturnCodeSet(codes) => codes.toList.sorted.mkString(",")
      }),
      ("cpu", runtimeAttributes.cpu.toString),
      ("preemptible", runtimeAttributes.preemptible.toString),
      ("disks", runtimeAttributes.disks.sortWith((l, r) => l.name > r.name).map(_.toString).mkString(",")),
      ("memoryGB", runtimeAttributes.memoryGB.toString)
    )

    val overallHash = Seq(
      backend.backendType.toString,
      call.task.commandTemplateString,
      orderedInputs map { case (k, v) => s"$k=${v.computeHash(workflowDescriptor.fileHasher).value}" } mkString "\n",
      orderedRuntime map { case (k, v) => s"$k=$v" } mkString "\n",
      orderedOutputs map { o => s"${o.wdlType.toWdlString} ${o.name} = ${o.requiredExpression.toWdlString}" } mkString "\n"
    ).mkString("\n---\n").md5Sum

    ExecutionHash(overallHash, dockerHash)
  }

  /**
    * Compute a hash that uniquely identifies this call
    */
  def hash(implicit ec: ExecutionContext): Future[ExecutionHash] = {
    // If a Docker image is defined in the task's runtime attributes, return a `Future[Option[String]]` of the Docker
    // hash string, otherwise return a `Future.successful` of `None`.
    def hashDockerImage(dockerImage: String): Future[Option[String]] =
      if (workflowDescriptor.lookupDockerHash)
        backend.dockerHashClient.getDockerHash(dockerImage) map { dh => Option(dh.hashString) }
      else
        Future.successful(Option(dockerImage))

    if (workflowDescriptor.configCallCaching)
      runtimeAttributes.docker map hashDockerImage getOrElse Future.successful(None) map hashGivenDockerHash
    else
      Future.successful(ExecutionHash("", None))
  }
}

final case class FinalCallJobDescriptor(workflowDescriptor: WorkflowDescriptor,
                                        key: FinalCallKey,
                                        workflowMetadataResponse: WorkflowMetadataResponse) extends JobDescriptor[FinalCallKey] {

  override val locallyQualifiedInputs = Map.empty[String, WdlValue]
}

