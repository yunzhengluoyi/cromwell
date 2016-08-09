package cromwell

import akka.testkit.EventFilter
import cromwell.core.WorkflowFailed
import cromwell.util.SampleWdl.WorkflowFailSlow
import org.scalatest.{FlatSpec, Matchers}

/*
  FLAGGED: This behavior is already covered by Centaur.
 */
class WorkflowFailSlowSpec extends FlatSpec with Matchers {
  import WorkflowFailSlowSpec._
  import NewFandangledTestThing.withTestThing

  behavior of "A workflow containing a failing task"

  it should "complete other tasks but ultimately fail, for ContinueWhilePossible" in {
    withTestThing {
      _.testWdl(WorkflowFailSlow,
        workflowOptions = FailSlowOptions,
        terminalState = WorkflowFailed,
        eventFilter = WorkflowFailSlowEventFilter(1))
    }
  }

  it should "not complete any other tasks and ultimately fail, for NoNewCalls" in {
    withTestThing {
      _.testWdl(WorkflowFailSlow,
        workflowOptions = FailFastOptions,
        terminalState = WorkflowFailed,
        eventFilter = WorkflowFailSlowEventFilter(0))
    }
  }

  it should "behave like NoNewCalls, if no workflowFailureMode is set" in {
    withTestThing {
      _.testWdl(WorkflowFailSlow, terminalState = WorkflowFailed, eventFilter = WorkflowFailSlowEventFilter(0))
    }
  }
}

object WorkflowFailSlowSpec {
  val FailSlowOptions =
    """
      |{
      |  "workflow_failure_mode": "ContinueWhilePossible"
      |}
    """.stripMargin

  val FailFastOptions =
    """
      |{
      |  "workflow_failure_mode": "NoNewCalls"
      |}
    """.stripMargin

  def WorkflowFailSlowEventFilter(n: Int) = EventFilter.info(pattern = "Job wf.E:NA:1 succeeded", occurrences = n)
}
