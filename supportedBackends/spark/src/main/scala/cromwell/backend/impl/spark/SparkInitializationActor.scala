package cromwell.backend.impl.spark

import akka.actor.Props
import cromwell.backend.validation.RuntimeAttributesKeys._
import cromwell.backend.impl.spark.SparkInitializationActor._
import cromwell.backend.{BackendConfigurationDescriptor, BackendWorkflowDescriptor, BackendWorkflowInitializationActor}
import wdl4s.types.{WdlBooleanType, WdlStringType}
import wdl4s.{WdlExpression, Call}

import scala.concurrent.Future

object SparkInitializationActor {
  val SupportedKeys = Set(FailOnStderrKey, SparkRuntimeAttributes.ExecutorCoresKey, SparkRuntimeAttributes.ExecutorMemoryKey,
    SparkRuntimeAttributes.NumberOfExecutorsKey, SparkRuntimeAttributes.AppMainClassKey, SparkRuntimeAttributes.SparkDeployMode,
    SparkRuntimeAttributes.SparkMaster)

  def props(workflowDescriptor: BackendWorkflowDescriptor, calls: Seq[Call], configurationDescriptor: BackendConfigurationDescriptor): Props =
    Props(new SparkInitializationActor(workflowDescriptor, calls, configurationDescriptor))
}

class SparkInitializationActor(override val workflowDescriptor: BackendWorkflowDescriptor,
                               override val calls: Seq[Call],
                               override val configurationDescriptor: BackendConfigurationDescriptor) extends BackendWorkflowInitializationActor {

  override protected def runtimeAttributeValidators: Map[String, (Option[WdlExpression]) => Boolean] =  Map(
    FailOnStderrKey -> wdlTypePredicate(valueRequired = false, WdlBooleanType.isCoerceableFrom),
    SparkRuntimeAttributes.AppMainClassKey -> wdlTypePredicate(valueRequired = true, WdlBooleanType.isCoerceableFrom),
    SparkRuntimeAttributes.NumberOfExecutorsKey -> wdlTypePredicate(valueRequired = false, WdlBooleanType.isCoerceableFrom),
    SparkRuntimeAttributes.ExecutorMemoryKey -> wdlTypePredicate(valueRequired = false, WdlBooleanType.isCoerceableFrom),
    SparkRuntimeAttributes.ExecutorCoresKey -> wdlTypePredicate(valueRequired = false, WdlBooleanType.isCoerceableFrom),
    SparkRuntimeAttributes.SparkDeployMode -> wdlTypePredicate(valueRequired = false, WdlBooleanType.isCoerceableFrom),
    SparkRuntimeAttributes.SparkMaster -> wdlTypePredicate(valueRequired = false, WdlBooleanType.isCoerceableFrom)
  )

  /**
    * Abort all initializations.
    */
  override def abortInitialization(): Unit = throw new UnsupportedOperationException("aborting initialization is not supported")

  /**
    * A call which happens before anything else runs
    */
  override def beforeAll(): Future[Unit] = Future.successful(())


  /**
    * A call which happens before anything else runs
    */
  override def validate(): Future[Unit] = {
    Future {
      calls foreach { call =>
        val runtimeAttributes = call.task.runtimeAttributes.attrs
        val notSupportedAttributes = runtimeAttributes filterKeys { !SupportedKeys.contains(_) }
        if (notSupportedAttributes.nonEmpty) {
          val notSupportedAttrString = notSupportedAttributes.keys mkString ", "
          log.warning(s"Key/s [$notSupportedAttrString] is/are not supported by SparkBackend. Unsupported attributes will not be part of jobs executions.")
        }
      }
    }
  }
}
