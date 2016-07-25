package cromwell.database.obj

case class BackendExecution
(
  backendKVStoreID: Int,
  workflowExecutionUuid: String,
  callFqn: String,
  index: Int,
  attempt: Int,
  backendJobKey: String,
  backendJobValue: String
)
