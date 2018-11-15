package cromwell.engine.workflow.workflowstore

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import com.google.common.cache.CacheBuilder
import cromwell.core.WorkflowId
import cromwell.engine.workflow.workflowstore.AbortRequestScanningActor.PerformScan
import cromwell.engine.workflow.workflowstore.WorkflowStoreActor.{FindWorkflowsIdsWithAbortRequestedFailure, FindWorkflowIdsWithAbortRequestedSuccess, FindWorkflowsWithAbortRequested}
import cromwell.engine.workflow.{WorkflowActor, WorkflowManagerActor}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

class AbortRequestScanningActor(workflowStoreActor: ActorRef, workflowManagerActor: ActorRef,workflowHeartbeatConfig: WorkflowHeartbeatConfig) extends Actor with Timers with ActorLogging {
  private val cache = CacheBuilder.newBuilder()
    .concurrencyLevel(1)
    .expireAfterWrite(1L, TimeUnit.HOURS)
    .build[WorkflowId, java.lang.Boolean]()

  self ! PerformScan

  override def receive: Receive = {
    case PerformScan =>
      workflowStoreActor ! FindWorkflowsWithAbortRequested
    case FindWorkflowIdsWithAbortRequestedSuccess(ids) =>
      val oldIds = cache.getAllPresent(ids.asJava).keySet().asScala
      // Diff this list to the cache.
      val newIds = ids.toSet -- oldIds
      workflowManagerActor ! WorkflowManagerActor.QueryRootWorkflowActorsRequest(newIds)
    case FindWorkflowsIdsWithAbortRequestedFailure(t) =>
      // Make a note of this misfortune.
      log.error(t, "Error searching for abort requests")
      // Don't get down, pick ourselves up off the floor! Restart that timer.
      startTimer()
    case WorkflowManagerActor.QueryRootWorkflowActorsResponse(actors, request) =>
      // IDs that the WMA allegedly does not recognize. Not necessarily a glitch in the Matrix, these might belong to
      // workflows that terminated between the time abort was requested and the abort request scan.
      val unrecognizedIds = request.ids -- actors.keySet
      log.info(s"Workflow IDs for abort not recognized by WorkflowManagerActor: ${unrecognizedIds.mkString(", ")}")
      actors.values.flatten foreach { _ ! WorkflowActor.AbortWorkflowCommand }
      // Add these to the cache so we don't ask them to abort again.
      actors.keys foreach { cache.put(_, true) }
      // In the knowledge of a job well done, restart the timer.
      startTimer()
  }

  private def startTimer(): Unit = timers.startSingleTimer(PerformScan, PerformScan, 1 minute)
}

object AbortRequestScanningActor {
  case object PerformScan

  def props(workflowStoreActor: ActorRef, workflowManagerActor: ActorRef, workflowHeartbeatConfig: WorkflowHeartbeatConfig): Props =
    Props(new AbortRequestScanningActor(workflowStoreActor, workflowManagerActor, workflowHeartbeatConfig))
}
