package cromwell.backend.impl.spark

import cromwell.backend.MemorySize
import cromwell.backend.validation.RuntimeAttributesDefault._
import cromwell.backend.validation.RuntimeAttributesKeys._
import cromwell.backend.validation.RuntimeAttributesValidation._
import cromwell.core._
import lenthall.exception.MessageAggregation
import wdl4s.types.{WdlBooleanType, WdlIntegerType, WdlStringType, WdlType}
import wdl4s.values.{WdlBoolean, WdlInteger, WdlString, WdlValue}

import scalaz.Scalaz._
import scalaz._

object SparkRuntimeAttributes {
  private val FailOnStderrDefaultValue = false
  private val ExecutorCoresDefaultValue = 1
  private val ExecutorMemoryDefaultValue = "1 GB"
  private val SparkMasterDefaultValue = "local"

  val ExecutorCoresKey = "executorCores"
  val ExecutorMemoryKey = "executorMemory"
  val AppMainClassKey = "appMainClass"
  //Specific to cluster mode
  val NumberOfExecutorsKey = "numberOfExecutors"
  val SparkDeployMode = "deployMode"
  val SparkMaster = "master"

  val staticDefaults = Map(
    FailOnStderrKey -> WdlBoolean(FailOnStderrDefaultValue),
    ExecutorCoresKey -> WdlInteger(ExecutorCoresDefaultValue),
    ExecutorMemoryKey -> WdlString(ExecutorMemoryDefaultValue),
    SparkMaster -> WdlString(SparkMasterDefaultValue)
  )

  val coercionMap: Map[String, Set[WdlType]] = Map(
    FailOnStderrKey -> Set[WdlType](WdlBooleanType),
    ExecutorCoresKey -> Set(WdlIntegerType),
    ExecutorMemoryKey -> Set(WdlStringType),
    AppMainClassKey -> Set(WdlStringType),
    NumberOfExecutorsKey -> Set(WdlIntegerType),
    SparkDeployMode -> Set(WdlStringType),
    SparkMaster -> Set(WdlStringType)
  )

  def apply(attrs: Map[String, WdlValue], options: WorkflowOptions): SparkRuntimeAttributes = {
    // Fail now if some workflow options are specified but can't be parsed correctly
    val defaultFromOptions = workflowOptionsDefault(options, coercionMap).get
    val withDefaultValues = withDefaults(attrs, List(defaultFromOptions, staticDefaults))

    val failOnStderr = validateFailOnStderr(withDefaultValues.get(FailOnStderrKey), noValueFoundFor(FailOnStderrKey))

    val executorCores = validateCpu(withDefaultValues.get(ExecutorCoresKey), noValueFoundFor(ExecutorCoresKey))
    val executorMemory = validateMemory(withDefaultValues.get(ExecutorMemoryKey), noValueFoundFor(ExecutorMemoryKey))
    val numberOfExecutors = validateNumberOfExecutors(withDefaultValues.get(NumberOfExecutorsKey), None.successNel)
    val appMainCLass = validateAppEntryPoint(withDefaultValues(AppMainClassKey))
    val deployMode = validateSparkDeployMode(withDefaultValues.get(SparkDeployMode), None.successNel)
    val sparkMaster = validateSparkMaster(withDefaultValues.get(SparkMaster), None.successNel)

    (executorCores |@| executorMemory |@| numberOfExecutors |@| appMainCLass |@| deployMode |@| sparkMaster |@| failOnStderr) {
      new SparkRuntimeAttributes(_, _, _, _, _, _, _)
    } match {
      case Success(x) => x
      case Failure(nel) => throw new RuntimeException with MessageAggregation {
        override def exceptionContext: String = "Runtime attribute validation failed"

        override def errorMessages: Traversable[String] = nel.list
      }
    }
  }

  private def validateNumberOfExecutors(numOfExecutors: Option[WdlValue], onMissingKey: => ErrorOr[Option[Int]]): ErrorOr[Option[Int]] = {
    numOfExecutors match {
      case Some(i: WdlInteger) => Some(i.value.intValue()).successNel
      case None => onMissingKey
      case _ => s"Expecting $NumberOfExecutorsKey runtime attribute to be an Integer".failureNel
    }
  }

  private def validateAppEntryPoint(mainClass: WdlValue): ErrorOr[String] = {
    WdlStringType.coerceRawValue(mainClass) match {
      case scala.util.Success(WdlString(s)) => s.successNel
      case _ => s"Could not coerce $AppMainClassKey into a String".failureNel
    }
  }

  private def validateSparkDeployMode(sparkDeployMode: Option[WdlValue], onMissingKey: => ErrorOr[Option[String]]): ErrorOr[Option[String]] = {
    sparkDeployMode match {
      case Some(s: WdlString) => Some(s.value).successNel
      case None => onMissingKey
      case _ => s"Expecting $SparkDeployMode runtime attribute to be a String".failureNel
    }
  }

  private def validateSparkMaster(sparkMaster: Option[WdlValue], onMissingKey: => ErrorOr[Option[String]]): ErrorOr[Option[String]] = {
    sparkMaster match {
      case Some(s: WdlString) => Some(s.value).successNel
      case None => onMissingKey
      case _ => s"Expecting $SparkMaster runtime attribute to be a String".failureNel
    }
  }

}

case class SparkRuntimeAttributes(executorCores: Int, executorMemory: MemorySize, numberOfExecutors: Option[Int],
                                  appMainClass: String, deployMode: Option[String], sparkMaster: Option[String],
                                  failOnStderr: Boolean)

