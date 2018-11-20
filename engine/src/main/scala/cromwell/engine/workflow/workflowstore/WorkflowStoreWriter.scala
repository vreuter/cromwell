package cromwell.engine.workflow.workflowstore

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import cats.data.NonEmptyVector
import cromwell.core.WorkflowId

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

sealed trait WorkflowStoreWriter {
  def writeWorkflowHeartbeats(workflowIds: NonEmptyVector[WorkflowId])
                             (implicit ec: ExecutionContext): Future[Int]

  def fetchStartableWorkflows(maxWorkflows: Int, cromwellId: String, heartbeatTtl: FiniteDuration)
                             (implicit ec: ExecutionContext): Future[List[WorkflowToStart]]
}


case class UncoordinatedWorkflowStoreWriter(store: WorkflowStore) extends WorkflowStoreWriter {

  override def writeWorkflowHeartbeats(workflowIds: NonEmptyVector[WorkflowId])
                                      (implicit ec: ExecutionContext): Future[Int] = {
    store.writeWorkflowHeartbeats(workflowIds.toVector.toSet)
  }

  override def fetchStartableWorkflows(maxWorkflows: Int, cromwellId: String, heartbeatTtl: FiniteDuration)
                                      (implicit ec: ExecutionContext): Future[List[WorkflowToStart]] = {
    store.fetchStartableWorkflows(maxWorkflows, cromwellId, heartbeatTtl)
  }
}

case class CoordinatedWorkflowStoreWriter(actor: ActorRef) extends WorkflowStoreWriter {
  override def writeWorkflowHeartbeats(workflowIds: NonEmptyVector[WorkflowId])
                                      (implicit ec: ExecutionContext): Future[Int] = {
    implicit val timeout = Timeout(WorkflowStoreCoordinatedWriteActor.Timeout)
    actor.ask(WorkflowStoreCoordinatedWriteActor.WriteHeartbeats(workflowIds)).mapTo[Int]
  }

  override def fetchStartableWorkflows(maxWorkflows: Int, cromwellId: String, heartbeatTtl: FiniteDuration)
                                      (implicit ec: ExecutionContext): Future[List[WorkflowToStart]] = {
    implicit val timeout = Timeout(WorkflowStoreCoordinatedWriteActor.Timeout)
    val message = WorkflowStoreCoordinatedWriteActor.FetchStartableWorkflows(maxWorkflows, cromwellId, heartbeatTtl)
    actor.ask(message).mapTo[List[WorkflowToStart]]
  }
}

