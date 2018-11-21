package cromwell.engine.workflow

import akka.testkit.TestKit
import cromwell.engine.workflow.workflowstore.{CoordinatedWorkflowStoreWriter, WorkflowStore, CoordinatedWorkflowStoreAccessActor}

trait CoordinatedWorkflowStoreBuilder { testKit: TestKit =>
  def writer(store: WorkflowStore): CoordinatedWorkflowStoreWriter = {
    val coordinatedWriteActor = testKit.system.actorOf(CoordinatedWorkflowStoreAccessActor.props(store))
    CoordinatedWorkflowStoreWriter(coordinatedWriteActor)
  }
}
