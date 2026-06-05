package what.is.on.eire

import cats.Applicative
import cats.syntax.functor._
import cats.syntax.traverse._

/** Processes a batch of JSON records (from a Kinesis trigger) and persists each valid event via an
  * injected save function.
  *
  * Takes a single function as a constructor parameter:
  *   - `saveEvent`: persists a single [[IrishEvent]] and returns a status message
  *
  * The `Applicative[F]` context bound allows us to use `.map` on `F` values and lift pure values
  * with `Applicative[F].pure`. In production `F = IO`; in tests `F = IO` with mock functions.
  *
  * @tparam F
  *   the effect type (e.g. `IO`)
  * @param saveEvent
  *   persists a single event, returns a descriptive string
  */
class EventStoreService[F[_]: Applicative](
  saveEvent: IrishEvent => F[String]
) {

  /** Parse each JSON record and save the valid ones.
    *
    * @param jsonRecords
    *   a list of raw JSON strings, one per Kinesis record
    * @return
    *   a summary of how many were saved vs. failed to parse
    */
  def processBatch(jsonRecords: List[String]): F[String] = {
    val results = jsonRecords.map { json =>
      KinesisRecordParser.parse(json) match {
        case Some(event) =>
          saveEvent(event).map(savedMsg => s"SAVED: $savedMsg")
        case None        =>
          val preview = json.take(80).replace("\n", " ")
          Applicative[F].pure(s"SKIPPED: could not parse record: $preview")
      }
    }
    combineResults(results)
  }

  /** Combine a list of F[String] results into a single F[String] summary. */
  private def combineResults(results: List[F[String]]): F[String] =
    results.sequence.map { msgs =>
      val saved   = msgs.count(_.startsWith("SAVED"))
      val skipped = msgs.count(_.startsWith("SKIPPED"))
      s"Processed ${msgs.size} records: $saved saved, $skipped skipped"
    }

}
