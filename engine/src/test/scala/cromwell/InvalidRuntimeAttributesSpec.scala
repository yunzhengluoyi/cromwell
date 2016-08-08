package cromwell

import org.scalatest.{FlatSpec, Matchers}
import cromwell.util.SampleWdl.HelloWorld

// FIXME (for code review): This is covered by an existing Centaur test, I believe it should go away
class InvalidRuntimeAttributesSpec extends FlatSpec with Matchers {
  import NewFandangledTestThing.withTestThing

  behavior of "InvalidRuntimeAttributes"

  it should "succeed a workflow with a task with one invalid runtime attribute" in {
    withTestThing { _.testWdl(sampleWdl = HelloWorld, runtime = """ runtime { wrongAttribute: "nop" }""".stripMargin) }
  }
}
