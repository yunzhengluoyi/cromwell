package cromwell.engine

import cromwell.core.Tags._
import cromwell.core.FullyQualifiedName
import cromwell.util.SampleWdl.CurrentDirectory
import org.scalatest.{FlatSpec, Matchers}
import wdl4s.values.WdlValue

import scala.language.postfixOps

class WorkflowManagerActorSpec extends FlatSpec with Matchers {
  import cromwell.NewFandangledTestThing.withTestThing

  behavior of "WorkflowManagerActor"

  // TODO PBE: Restart workflows tests: re-add (but somewhere else?) in 0.21
  it should "not try to restart any workflows when there are no workflows in restartable states" taggedAs PostMVP ignore {}
  it should "try to restart workflows when there are workflows in restartable states" taggedAs PostMVP ignore {}

  it should "run workflows in the correct directory" in {
    def checkOutputs(outputs: Map[FullyQualifiedName, WdlValue]): Boolean = {
      val outputName = "whereami.whereami.pwd"
      val salutation = outputs(outputName)
      val actualOutput = salutation.valueString.trim
      actualOutput.endsWith("/call-whereami")
    }

    withTestThing { _.testWdl(CurrentDirectory, outputCheck = Option(checkOutputs)) }
  }
}
