package cromwell.backend.validation

import cromwell.core.{ErrorOr, EvaluatedRuntimeAttributes, OptionNotFoundException, WorkflowOptions}
import wdl4s.types.WdlType
import wdl4s.util.TryUtil
import wdl4s.values.WdlValue

import scala.util.{Failure, Success}
import scalaz.Scalaz._

object RuntimeAttributesDefault {

  def workflowOptionsDefault(options: WorkflowOptions, mapping: Map[String, Traversable[WdlType]]): ErrorOr[Map[String, WdlValue]] = {
    val tryMap = options.defaultRuntimeOptions flatMap { attrs =>
      TryUtil.sequenceMap(attrs collect {
        case (k, v) if mapping.contains(k) =>
          val maybeTriedValue = mapping(k) map {  _.coerceRawValue(v) } find { _.isSuccess } getOrElse {
            Failure(new RuntimeException(s"Could not parse JsonValue $v to valid WdlValue for runtime attribute $k"))
          }
          k -> maybeTriedValue
      })
    }

    tryMap match {
      case Success(m) => m.successNel
      case Failure(t: OptionNotFoundException) => Map.empty[String, WdlValue].successNel
      case Failure(t) => t.getMessage.failureNel
    }
  }

  /**
    * Traverse defaultsList in order, and for each of them add the missing (and only missing) runtime attributes.
   */
  def withDefaults(attrs: EvaluatedRuntimeAttributes, defaultsList: List[EvaluatedRuntimeAttributes]): EvaluatedRuntimeAttributes = {
    defaultsList.foldLeft(attrs)((acc, default) => {
      acc ++ default.filterKeys(!acc.keySet.contains(_))
    })
  }

  def noValueFoundFor[A](attribute: String): ErrorOr[A] = s"Can't find an attribute value for key $attribute".failureNel
}
