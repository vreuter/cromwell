package cromwell.engine

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorRef
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import common.validation.ErrorOr.ErrorOr
import cromwell.core.CacheConfig
import wdl.draft2.model._

import scala.util.{Failure, Success, Try}

final case class AbortFunction(function: () => Unit)
final case class AbortRegistrationFunction(register: AbortFunction => Unit)

final case class CallAttempt(fqn: FullyQualifiedName, attempt: Int)

object WorkflowFailureMode {
  def tryParse(mode: String): Try[WorkflowFailureMode] = {
    val modes = Seq(ContinueWhilePossible, NoNewCalls)
    modes find { _.toString.equalsIgnoreCase(mode) } map { Success(_) } getOrElse Failure(new Exception(s"Invalid workflow failure mode: $mode"))
  }
}
sealed trait WorkflowFailureMode {
  def allowNewCallsAfterFailure: Boolean
}
case object ContinueWhilePossible extends WorkflowFailureMode { override val allowNewCallsAfterFailure = true }
case object NoNewCalls extends WorkflowFailureMode { override val allowNewCallsAfterFailure = false }

case class SubWorkflowStart(actorRef: ActorRef)

case class FileHashCacheValue(requested: AtomicBoolean, hashValue: Option[ErrorOr[String]])

case class FileHashCache(c: CacheConfig) {
  val guavaCache: LoadingCache[String, FileHashCacheValue] =
    CacheBuilder.newBuilder()
    .concurrencyLevel(c.concurrency)
    .expireAfterAccess(c.ttl.length, c.ttl.unit)
    .maximumSize(c.size)
    .build[String, FileHashCacheValue](new CacheLoader[String, FileHashCacheValue] {
    override def load(key: String): FileHashCacheValue =
      FileHashCacheValue(
        requested = new AtomicBoolean(false),
        hashValue = None
      )
  })

  def isFirstHashRequest(key: String): Boolean = {
    guavaCache.get(key).requested.compareAndSet(false, true)
  }

  def update(key: String, value: ErrorOr[String]): Unit =
    guavaCache.put(key, FileHashCacheValue(requested = new AtomicBoolean(true), hashValue = Option(value)))
}
