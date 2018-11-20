package cromwell.engine.workflow

import akka.testkit.TestKit
import cromwell.engine.workflow.workflowstore.{CoordinatedWorkflowStoreWriter, WorkflowStore, WorkflowStoreCoordinatedWriteActor}

trait CoordinatedWorkflowStoreBuilder { testKit: TestKit =>
  def writer(store: WorkflowStore): CoordinatedWorkflowStoreWriter = {
    val coordinatedWriteActor = testKit.system.actorOf(WorkflowStoreCoordinatedWriteActor.props(store))
    CoordinatedWorkflowStoreWriter(coordinatedWriteActor)
  }
}
