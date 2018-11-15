package cromwell.core.abort

import cromwell.core.WorkflowId

sealed trait AbortResponse
case class WorkflowAbortRequestFailureResponse(workflowId: WorkflowId, failure: Throwable) extends AbortResponse
case class WorkflowAbortRequestSuccessResponse(workflowId: WorkflowId) extends AbortResponse
