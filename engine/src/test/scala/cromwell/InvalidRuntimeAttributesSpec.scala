package cromwell

import org.scalatest.{FlatSpec, Matchers}
import cromwell.util.SampleWdl.HelloWorld

/*
  FLAGGED: This behavior is covered by Centaur already. This should be morphed into a proper unit test, if it isn't
  already somewhere
 */
class InvalidRuntimeAttributesSpec extends FlatSpec with Matchers {
  import NewFandangledTestThing.withTestThing

  behavior of "InvalidRuntimeAttributes"

  it should "succeed a workflow with a task with one invalid runtime attribute" in {
    withTestThing { _.testWdl(sampleWdl = HelloWorld, runtime = """ runtime { wrongAttribute: "nop" }""".stripMargin) }
  }
}
