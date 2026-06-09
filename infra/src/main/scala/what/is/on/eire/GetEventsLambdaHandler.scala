package what.is.on.eire

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets

/** Lambda handler that reads events from DynamoDB, enriches them from rawPayload, and returns
  * [[EnrichedEvent]] records for the frontend.
  *
  * Expects a JSON payload with an optional `irish-location` header:
  * {{{
  *   { "headers": { "irish-location": "Dublin" }, "body": "{}" }
  * }}}
  * If the header is present, only events matching that city are returned. If absent, all events are
  * returned.
  */
class GetEventsLambdaHandler extends RequestStreamHandler {

  private given runtime: IORuntime = IORuntime.global

  private val tableName: String =
    Option(System.getenv("EVENTS_TABLE_NAME"))
      .getOrElse("irish-events")

  private val localstackPort: Option[Int] =
    EnvLoader.get("LOCALSTACK_PORT").flatMap(p => scala.util.Try(p.toInt).toOption)

  override def handleRequest(
    input: InputStream,
    output: OutputStream,
    context: Context
  ): Unit = {
    val logger = context.getLogger
    logger.log("WhatsOnEire GetEvents Lambda invoked!")

    val rawInput = new String(input.readAllBytes(), StandardCharsets.UTF_8)
    logger.log(s"Incoming payload (first 200 chars): ${rawInput.take(200)}")

    val city = CityHeaderParser.parse(rawInput)
    logger.log(s"City filter: ${city.getOrElse("ALL")}")

    try {
      val enriched = DynamoDbEventRepository
        .resource(tableName, localstackPort)
        .use { repo =>
          repo.getEvents(city)
        }
        .map { events =>
          val enrichment = new EnrichmentService(List(new TicketmasterEnricher))
          events.map(enrichment.enrich)
        }
        .unsafeRunSync()

      logger.log(s"Found ${enriched.size} events, enriched for frontend")

      import smithy4s.Schema

      val listSchema = smithy4s.schema.Schema.list(Schema[EnrichedEvent])
      val encoder    = smithy4s.json.Json.payloadCodecs.encoders.fromSchema(listSchema)
      val blob       = encoder.encode(enriched)
      val jsonBytes  = blob.toArray
      output.write(jsonBytes)
    } catch {
      case e: Exception =>
        logger.log(s"ERROR: ${e.getMessage}\n${e.getStackTrace.map(_.toString).mkString("\n")}")
        val error = s"""{"status": "FAILED", "message": "${e.getMessage}"}"""
        output.write(error.getBytes(StandardCharsets.UTF_8))
    }
  }

}
