package what.is.on.eire

import java.nio.charset.StandardCharsets
import smithy4s.Blob
import smithy4s.Schema
import smithy4s.json.Json

/** Pure utility that deserialises a JSON string (from a single Kinesis record) into an
  * [[IrishEvent]] case class.
  *
  * Expects the JSON shape produced by [[KinesisEventPublisher.serializeEvent]], which uses smithy4s
  * generated JSON codecs.
  *
  * Only `id`, `title`, `city`, `county`, `source`, and `rawPayload` are hard-required for a
  * successful parse. Fields `url`, `startDate`, `startTime`, and `coordinates` are optional and
  * default to empty string or `None` when absent. The `county` field tolerates any string value —
  * unrecognised values fall back to [[IrishCounty.UNKNOWN]]. This ensures events are saved to the
  * database even if the source data is incomplete.
  *
  * Thread-safe: all parsing is stateless.
  */
object KinesisRecordParser {

  private val decoder = Json.payloadCodecs.decoders.fromSchema(Schema[IrishEvent])

  def parse(json: String): Option[IrishEvent] = {
    val blob = Blob(json.getBytes(StandardCharsets.UTF_8))
    decoder.decode(blob).toOption
  }

}
