package cromwell.engine.backend.local

import cromwell.NewFandangledTestThing
import cromwell.core.WorkflowFailed
import cromwell.util.SampleWdl
import org.scalatest.{FlatSpec, Matchers}
import wdl4s.WdlSource

object LocalBackendSpec {
  object StdoutWdl extends SampleWdl {
    override def wdlSource(runtime: String): WdlSource =
      """task out { command { echo beach } RUNTIME }
        |workflow wf { call out }
      """.stripMargin.replaceAll("RUNTIME", runtime)
    override val rawInputs =  Map.empty[String, String]
  }

  object StderrWdl extends SampleWdl {
    override def wdlSource(runtime: String): WdlSource =
      """task err { command { echo beach >&2 } RUNTIME }
        |workflow wf { call err }
      """.stripMargin.replaceAll("RUNTIME", runtime)
    override val rawInputs =  Map.empty[String, String]
  }
}

class LocalBackendSpec extends FlatSpec with Matchers {
  import LocalBackendSpec._
  import cromwell.NewFandangledTestThing.withTestThing

  behavior of "LocalBackend"

  it should "allow stdout if failOnStderr is set" in {
    withTestThing { _.testWdl(sampleWdl = StdoutWdl, runtime = "runtime { failOnStderr: true }") }
  }

  it should "not allow stderr if failOnStderr is set" in {
    withTestThing {
      _.testWdl(sampleWdl = StderrWdl,
        eventFilter = NewFandangledTestThing.workflowFailureFilter,
        runtime = "runtime {failOnStderr: true}",
        terminalState = WorkflowFailed)
    }
  }

  it should "allow stdout if failOnStderr is not set" in {
    withTestThing { _.testWdl(sampleWdl = StdoutWdl, runtime = "runtime {failOnStderr: false}") }
  }

  it should "allow stderr if failOnStderr is not set" in {
    withTestThing { _.testWdl(sampleWdl = StderrWdl, runtime = "runtime {failOnStderr: false}") }
  }
}
