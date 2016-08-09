package cromwell

import cromwell.core.WorkflowFailed
import cromwell.util.SampleWdl.ContinueOnReturnCode
import org.scalatest.{FlatSpec, Matchers}

/*
  FLAGGED: This test is nearly fully handled by Centaur already and could be fully replicated easily. This is also
  testing workflow-level behavior, not a unit
 */

class ContinueOnReturnCodeWorkflowSpec extends FlatSpec with Matchers {
  import NewFandangledTestThing.{withTestThing, workflowFailureFilter}

  behavior of "A workflow with tasks that produce non-zero return codes"

  it should "fail if the return code is undefined in the continueOnReturnCode runtime attribute and the return code is non zero" in {
    withTestThing { _.testWdl(ContinueOnReturnCode, eventFilter = workflowFailureFilter, terminalState = WorkflowFailed) }
  }

  it should "fail if the return code is false in the continueOnReturnCode runtime attribute and the return code is non zero" in {
    withTestThing {
      _.testWdl(ContinueOnReturnCode, runtime = "runtime {continueOnReturnCode: false}", eventFilter = workflowFailureFilter, terminalState = WorkflowFailed)
    }
  }

  it should "succeed if the return code is true in the continueOnReturnCode runtime attribute" in {
    withTestThing { _.testWdl(ContinueOnReturnCode, runtime = "runtime {continueOnReturnCode: true}") }
  }

  it should "succeed if the return code is defined in the continueOnReturnCode runtime attribute" in {
    withTestThing { _.testWdl(ContinueOnReturnCode, runtime = "runtime {continueOnReturnCode: 123}") }
  }

  it should "succeed if the return code is present in the continueOnReturnCode runtime attributes list" in {
    withTestThing { _.testWdl(ContinueOnReturnCode, runtime = "runtime {continueOnReturnCode: [123]}") }
  }
}
