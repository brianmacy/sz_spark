package com.senzing.spark.work

import com.senzing.sdk._
import org.scalatest.funsuite.AnyFunSuite

final class ErrorTaxonomySpec extends AnyFunSuite {
  import ErrorCategory._

  test("subclasses classify before their supertypes (most-specific-first)") {
    assert(ErrorTaxonomy.classify(new SzUnknownDataSourceException("x")) == ConfigRelevant)
    assert(ErrorTaxonomy.classify(new SzNotFoundException("x")) == NotFound)
    assert(ErrorTaxonomy.classify(new SzBadInputException("x")) == BadInput)
    assert(ErrorTaxonomy.classify(new SzRetryTimeoutExceededException("x")) == RetryExhausted)
    assert(ErrorTaxonomy.classify(new SzDatabaseConnectionLostException("x")) == Retryable)
    assert(ErrorTaxonomy.classify(new SzRetryableException("x")) == Retryable)
  }

  test("config-relevant and replace-conflict") {
    assert(ErrorTaxonomy.classify(new SzConfigurationException("x")) == ConfigRelevant)
    assert(ErrorTaxonomy.classify(new SzReplaceConflictException("x")) == ReplaceConflict)
    assert(ErrorTaxonomy.isConfigRelevant(ConfigRelevant))
    assert(ErrorTaxonomy.isConfigRelevant(ReplaceConflict))
    assert(!ErrorTaxonomy.isConfigRelevant(BadInput))
  }

  test("unrecoverable family and anything else are systemic") {
    assert(ErrorTaxonomy.classify(new SzLicenseException("x")) == Systemic)
    assert(ErrorTaxonomy.classify(new SzNotInitializedException("x")) == Systemic)
    assert(ErrorTaxonomy.classify(new SzDatabaseException("x")) == Systemic)
    assert(ErrorTaxonomy.classify(new SzUnhandledException("x")) == Systemic)
    assert(ErrorTaxonomy.classify(new SzUnrecoverableException("x")) == Systemic)
    assert(ErrorTaxonomy.classify(new SzException("x")) == Systemic)
    assert(ErrorTaxonomy.classify(new RuntimeException("x")) == Systemic)
    assert(ErrorTaxonomy.classify(new OutOfMemoryError("x")) == Systemic)
  }

  test("errorCode is extracted when present") {
    assert(ErrorTaxonomy.errorCode(new SzException(28, "m")) == "28")
    assert(ErrorTaxonomy.errorCode(new RuntimeException("m")) == "")
  }
}
