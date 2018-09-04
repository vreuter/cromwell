package cromwell.core.retry

import cats.effect.{IO, Timer}
import cats.syntax.all._
import cromwell.core.CromwellFatalException

import scala.concurrent.duration._

object IORetry {
  def noOpOnRetry[S]: (Throwable, S) => S = (_, s) => s

  case class StatefulIoException[S](state: S, cause: Throwable) extends CromwellFatalException(cause)

  /**
    * Same as Retry.withRetry using cats.effect.IO
    */
  def withRetry[A, S](io: IO[A],
                      state: S,
                      maxRetries: Option[Int],
                      backoff: Backoff = SimpleExponentialBackoff(5.seconds, 10.seconds, 1.1D),
                      isTransient: Throwable => Boolean = throwableToFalse,
                      isFatal: Throwable => Boolean = throwableToFalse,
                      onRetry: (Throwable, S) => S = noOpOnRetry)
                     (implicit timer: Timer[IO]): IO[A] = {
    val delay = backoff.backoffMillis.millis

    def fail(throwable: Throwable) = IO.raiseError(StatefulIoException(state, throwable))

    io handleErrorWith {
      case throwable if isFatal(throwable) => fail(throwable)
      case throwable =>
        val retriesLeft = if (isTransient(throwable)) maxRetries else maxRetries map { _ - 1 }

        if (retriesLeft.forall(_ > 0)) {
          IO.sleep(delay) *> withRetry(io, onRetry(throwable, state), retriesLeft, backoff.next, isTransient, isFatal, onRetry)
        }
        else fail(throwable)
    }

  }

  def throwableToFalse(t: Throwable) = false
}
