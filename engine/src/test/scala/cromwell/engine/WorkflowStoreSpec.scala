package cromwell.engine

import cromwell.engine.workflow.WorkflowStore
import cromwell.util.SampleWdl.HelloWorld
import org.scalatest.{FlatSpec, Matchers}

class WorkflowStoreSpec extends FlatSpec with Matchers with WorkflowStore {
  val sources = HelloWorld.asWorkflowSources()

  "A WorkflowStore" should "return an ID for a submitted workflow" in {
    val id = add(sources)
    dump.keySet shouldBe Set(id)
  }

  it should "return 3 IDs for a batch submission of 3" in {
    val origIds = dump.keySet
    val newIds = add(List(sources, sources, sources)).toSet
    newIds should have size 3
    dump.keySet shouldBe (origIds ++ newIds)
  }

  it should "fetch no more than N workflows" in {
    val origIds = dump.keySet
    val newIds = fetchRunnableWorkflows(1) map { _.id }
    newIds should have size 1
    val currentIds = dump.filter({ case (_, v) => v.state.isStartable }).keySet
    currentIds should be (origIds -- newIds)
  }

  it should "return only the remaining workflows if N is larger than size" in {
    val origIds = dump.filter({ case (_, v) => v.state.isStartable }).keySet
    val newIds = fetchRunnableWorkflows(origIds.size + 5) map { _.id }
    newIds should have size origIds.size
    val currentIds = dump.filter({ case (_, v) => v.state.isStartable }).keySet
    currentIds shouldBe empty
  }

  // remove
}
