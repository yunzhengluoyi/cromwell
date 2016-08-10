package cromwell.engine.workflow.lifecycle.execution.callcaching

import java.util.UUID

import akka.actor._
import akka.routing._
import cromwell.core.callcaching._
import cromwell.engine.workflow.lifecycle.execution.callcaching.DockerHashLookupActor.{DockerHashLookupCommand, DockerHashLookupKey, DockerHashLookupResponse}

import scala.util.{Failure, Success, Try}

class DockerHashLookupActor(actorCount: Int) extends Actor with ActorLogging {
  var router = {
    val routees = Vector.fill(actorCount) {
      val r = context.actorOf(Props[DockerHashLookupWorkerActor])
      context watch r
      ActorRefRoutee(r)
    }
    Router(RoundRobinRoutingLogic(), routees)
  }

  def receive = {
    case w: DockerHashLookupCommand =>
      router.route(w, sender())
    case Terminated(a) =>
      router = router.removeRoutee(a)
      val r = context.actorOf(Props[DockerHashLookupWorkerActor], s"DockerHashLookupWorker-${UUID.randomUUID.toString.substring(0,7)}")
      context watch r
      router = router.addRoutee(r)
    case x => log.error(s"Unexpected message to ${self.path.name}: $x")
  }
}

/**
  * Blocking worker. Warning! If this actor dies then its mailbox of hash requests will be lost
  */
private[callcaching] class DockerHashLookupWorkerActor extends Actor with ActorLogging {
  override def receive = {
    case x: DockerHashLookupCommand =>
      getDockerHash(x) match {
        case Success(dockerHashLookupSuccess) => sender ! DockerHashLookupResponse(HashResult(DockerHashLookupKey, HashValue(dockerHashLookupSuccess)))
        case Failure(t) => sender ! HashingFailedMessage(DockerHashLookupKey, t)
      }
  }

  def getDockerHash(lookupCommand: DockerHashLookupCommand): Try[String] = Failure(new NotImplementedError("No docker hash lookup yet"))
}

object DockerHashLookupActor {
  def props(actorCount: Int): Props = Props(new DockerHashLookupActor(actorCount))
  object DockerHashLookupKey extends HashKey("runtime attribute: docker (HASH)")
  case class DockerHashLookupCommand(name: String)
  case class DockerHashLookupResponse(hashResult: HashResult) extends SuccessfulHashResultMessage { override val hashes = Set(hashResult) }
}