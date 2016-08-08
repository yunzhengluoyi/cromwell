package cromwell

import cromwell.core.FullyQualifiedName
import cromwell.core.Tags.DockerTest
import wdl4s.values.{WdlString, WdlValue}
import cromwell.util.SampleWdl.GlobtasticWorkflow
import org.scalatest.{FlatSpec, Matchers}

import scala.language.postfixOps

// FIXME (for Code Review): This is covered (only for docker) by centaur. I believe this should go away
class GlobbingWorkflowSpec extends FlatSpec with Matchers {
  import NewFandangledTestThing.withTestThing

  behavior of "GlobbingWorkflow"

  it should "run properly" in doTheTest()

  it should "run properly in a Docker environment" taggedAs DockerTest in doTheTest("""runtime { docker: "ubuntu:latest" }""")

  def doTheTest(runtime: String = "") = withTestThing {
    _.testWdl(GlobtasticWorkflow, runtime = runtime, outputCheck = Option(checkOutputs))
  }

  def checkOutputs(outputs: Map[FullyQualifiedName, WdlValue]): Boolean = {
    // The order in which files glob is apparently not guaranteed, so accept any permutation.
    val permutations = for {
      permutation <- List('a', 'b', 'c').permutations
    } yield WdlString(permutation.mkString("\n"))

    val actual = outputs("w.B.B_out")
    permutations collectFirst { case s: WdlString if s == actual => s } isDefined
  }
}
