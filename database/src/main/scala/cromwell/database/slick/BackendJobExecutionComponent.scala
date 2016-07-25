package cromwell.database.slick


import cromwell.database.obj.BackendExecution

trait BackendJobExecutionComponent {
  //unclear what other Components are neccessary since this is a general KV store
  this: DriverComponent =>

  import driver.api._

  class BackendExecutions(tag: Tag) extends Table[BackendExecution](tag, "BackendExecution") {
    def backendKVStoreID = column[Int]("BACKEND_KV_STORE_ID", O.PrimaryKey, O.AutoInc)
    def workflowExecutionUuid = column[String]("WORKFLOW_EXECUTION_UUID")
    def callFqn = column[String]("CALL_FQN")
    def index = column[Int]("INDEX")
    def attempt = column[Int]("ATTEMPT")
    def backendJobKey = column[String]("BACKEND_JOB_KEY")
    def backendJobValue = column[String]("BACKEND_JOB_VALUE")


    override def * = (backendKVStoreID, workflowExecutionUuid, callFqn, index, attempt, backendJobKey, backendJobValue) <>
      (BackendExecution.tupled, BackendExecution.unapply)

      def jobIndex = index("BACKEND_JOB_EXECUTION_WORKFLOW_CALL_INDEX_ATTEMPT", (workflowExecutionUuid, callFqn, index, attempt), unique = false)

      def backendJobKeyIndex = index("BACKEND_JOB_EXECUTION_KEY_INDEX", (workflowExecutionUuid, callFqn, index, attempt, backendJobKey), unique = true)
    }

    protected val backendExecutions = TableQuery[BackendExecutions]

    val backendJobValueByBackendJobKey = Compiled(
      (workflowExecutionUuid: Rep[String], callFqn: Rep[String], index: Rep[Int], attempt: Rep[Int], backendJobKey: Rep[String]) => for {
      backendExecution <- backendExecutions
       if backendExecution.workflowExecutionUuid === workflowExecutionUuid
       if backendExecution.callFqn === callFqn
       if backendExecution.index === index
       if backendExecution.attempt === attempt
       if backendExecution.backendJobKey === backendJobKey
      } yield backendExecution)

    val backendJobKeyAndValueByWorkflowUuidAndCallFqnAndIndexAndAttempt = Compiled(
      (workflowExecutionUuid: Rep[String], callFqn: Rep[String], index: Rep[Int], attempt: Rep[Int]) => for {
        backendExecution <- backendExecutions
        if backendExecution.workflowExecutionUuid === workflowExecutionUuid
        if backendExecution.callFqn === callFqn
        if backendExecution.index === index
        if backendExecution.attempt === attempt
      } yield backendExecution)

    val backendJobKeyAndValueByWorkflowUuidAndCallFqn = Compiled(
      (workflowExecutionUuid: Rep[String], callFqn: Rep[String]) => for {
        backendExecution <- backendExecutions
        if backendExecution.workflowExecutionUuid === workflowExecutionUuid
        if backendExecution.callFqn === callFqn
      } yield backendExecution)
}