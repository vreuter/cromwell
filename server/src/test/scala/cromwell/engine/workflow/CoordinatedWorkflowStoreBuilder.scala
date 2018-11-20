package cromwell.engine.workflow

import akka.actor.ActorSystem
import cromwell.engine.workflow.workflowstore.{CoordinatedWorkflowStoreWriter, WorkflowStore, WorkflowStoreCoordinatedWriteActor}

trait CoordinatedWorkflowStoreBuilder {
  def buildCoordinatedWriter(system: ActorSystem)(store: WorkflowStore): CoordinatedWorkflowStoreWriter = {
    val coordinatedWriteActor = system.actorOf(WorkflowStoreCoordinatedWriteActor.props(store))
    CoordinatedWorkflowStoreWriter(coordinatedWriteActor)
  }
}
