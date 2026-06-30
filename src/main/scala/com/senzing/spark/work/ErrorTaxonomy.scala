package com.senzing.spark.work

import com.senzing.sdk._

/**
 * Disposition category for a per-record failure. `name` is the value written to ErrorRow.category.
 */
sealed abstract class ErrorCategory(val name: String)
object ErrorCategory {
  case object NotFound extends ErrorCategory("NOT_FOUND")
  case object ConfigRelevant extends ErrorCategory("CONFIG_RELEVANT")
  case object ReplaceConflict extends ErrorCategory("REPLACE_CONFLICT")
  case object BadInput extends ErrorCategory("BAD_INPUT")
  case object RetryExhausted extends ErrorCategory("RETRY_EXHAUSTED")
  case object Retryable extends ErrorCategory("RETRYABLE")
  case object Systemic extends ErrorCategory("SYSTEMIC")
}

/**
 * Classifies Senzing exceptions, MOST-SPECIFIC-SUBCLASS-FIRST, against the verified V4 hierarchy.
 * Ordering matters: `SzUnknownDataSource`/`SzNotFound` extend `SzBadInput`, and
 * `SzRetryTimeoutExceeded` extends `SzRetryable`, so the subclasses must match before their
 * supertypes.
 */
object ErrorTaxonomy {
  import ErrorCategory._

  def classify(t: Throwable): ErrorCategory = t match {
    case _: SzUnknownDataSourceException => ConfigRelevant // matched before SzBadInputException
    case _: SzNotFoundException => NotFound // matched before SzBadInputException
    case _: SzConfigurationException => ConfigRelevant
    case _: SzReplaceConflictException => ReplaceConflict
    case _: SzBadInputException => BadInput
    case _: SzRetryTimeoutExceededException => RetryExhausted // matched before SzRetryableException
    case _: SzRetryableException => Retryable
    case _ => Systemic // SzUnrecoverable family, other SzException, Error
  }

  /** Categories the worker may cure with a config-drift reinit + single retry. */
  def isConfigRelevant(c: ErrorCategory): Boolean =
    c == ConfigRelevant || c == ReplaceConflict

  /** The Senzing error code if available, else "". */
  def errorCode(t: Throwable): String = t match {
    case e: SzException => Option(e.getErrorCode).map(_.toString).getOrElse("")
    case _ => ""
  }
}
