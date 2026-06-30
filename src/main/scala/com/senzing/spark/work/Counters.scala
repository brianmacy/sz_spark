package com.senzing.spark.work

/** In-memory per-task counters (single-threaded worker). Never sourced from `engine.getStats`. */
final class Counters {
  var succeeded: Long = 0
  var skipped: Long = 0
  var notFound: Long = 0
  var errored: Long = 0
  var retried: Long = 0
  var long: Long = 0
  var replaceConflict: Long = 0

  def snapshot: Counters.Snapshot =
    Counters.Snapshot(succeeded, skipped, notFound, errored, retried, long, replaceConflict)
}

object Counters {
  final case class Snapshot(
      succeeded: Long,
      skipped: Long,
      notFound: Long,
      errored: Long,
      retried: Long,
      long: Long,
      replaceConflict: Long
  ) {
    def brief: String =
      s"succeeded=$succeeded skipped=$skipped notFound=$notFound errored=$errored " +
        s"retried=$retried long=$long replaceConflict=$replaceConflict"
  }
}
