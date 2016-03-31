package cromwell.engine.backend

import java.util.UUID

import akka.actor._
import cromwell.engine.backend.ExampleAbstractBackendActor._

object ExampleAbstractBackendActor {
    sealed trait BackendActorMessage
      case class Execute(wd : WorkflowDescriptor) extends BackendActorMessage
      case class Abort() extends BackendActorMessage
      case class AbortAll() extends BackendActorMessage

      // NOTE: doesn't recover need some state/parameters??
      case class Recover() extends BackendActorMessage

    // messages to be sent back to caller (WorkflowActor)
    sealed trait BackendActorStatusMessage
      case class Received(id : UUID) extends BackendActorStatusMessage  // Proposed
      case class Completed() extends BackendActorStatusMessage
      case class Failed() extends BackendActorStatusMessage

}

class ExampleAbstractBackendActor extends Actor {
  import context._

  def receive = {
    case message: Execute => handleExecute()
    case message: Abort => handleAbort()
    case message: AbortAll => handleAbortAll()
    case message: Recover => handleRecover()
    case x =>
      system.log.error("Unsupported response message sent to ExampleAbstractBackendActor actor: " + Option(x).getOrElse("null").toString)

      // how should unknown messages be additionally handled
  }

  abstract def handleExecute() = {
    val uuid = UUID.randomUUID()
    sender ! Received(uuid) // send back ReceivedMessage

    // doWork()

    // send back Completed/Failed Message
    sender ! Completed()
  }

  abstract def handleAbort() = {}
  abstract def handleAbortAll() = {}
  abstract def handleRecover() = {}

  // Contract is to use Actor Lifecycle methods for initialization and teardown
  override def preStart(): Unit = {}
  override def postStop(): Unit = {}

}


