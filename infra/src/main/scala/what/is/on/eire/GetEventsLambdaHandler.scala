package what.is.on.eire

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets

/** Lambda handler that reads events from DynamoDB, enriches them from rawPayload, and returns
  * [[EnrichedEvent]] records for the frontend with pagination.
  *
  * Expects a JSON payload with an optional `irish-location` header:
  * {{{
  *   { "headers": { "irish-location": "Dublin" }, "body": "{ \"page\": 2, \"pageSize\": 10 }" }
  * }}}
  * If the header is present, only events matching that city are returned. If absent, all events are
  * returned. Default pagination is page=0, pageSize=10.
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

    val (page, pageSize) = parsePagination(rawInput)
    logger.log(s"Pagination: page=$page, pageSize=$pageSize")

    try {
      val (enriched, totalCount) = DynamoDbEventRepository
        .resource(tableName, localstackPort)
        .use { repo =>
          repo.getEventsPaginated(city, page, pageSize)
        }
        .map { case (events, totalCount) =>
          val enrichment = new EnrichmentService(List(new TicketmasterEnricher))
          val enriched   = events.map(enrichment.enrich)
          (enriched, totalCount)
        }
        .unsafeRunSync()

      logger.log(s"Found $totalCount total events, returning page $page with ${enriched.size}")

      import smithy4s.Schema

      val eventsSchema = smithy4s.schema.Schema.list(Schema[EnrichedEvent])
      val encoder      = smithy4s.json.Json.payloadCodecs.encoders.fromSchema(eventsSchema)
      val eventsBlob   = encoder.encode(enriched)
      val eventsJson   = new String(eventsBlob.toArray, StandardCharsets.UTF_8)

      val totalPages = Math.ceil(totalCount.toDouble / pageSize).toInt
      val response   =
        s"""{"page":$page,"pageSize":$pageSize,"totalEvents":$totalCount,"totalPages":$totalPages,"events":$eventsJson}"""
      output.write(response.getBytes(StandardCharsets.UTF_8))
    } catch {
      case e: Throwable =>
        logger.log(s"ERROR: ${e.getMessage}\n${e.getStackTrace.map(_.toString).mkString("\n")}")
        val error = s"""{"status": "FAILED", "message": "${e.getMessage}"}"""
        output.write(error.getBytes(StandardCharsets.UTF_8))
    }
  }

  /** Parse pagination params from the request payload. Defaults to page=0, pageSize=10.
    *
    * Matches JSON keys `"page"` and `"pageSize"` at the top level. The double-quoted key names are
    * unambiguous — `"page":` cannot match inside `"pageSize":`.
    */
  private def parsePagination(rawInput: String): (Int, Int) = {
    val pageRegex     = """"page"\s*:\s*(\d+)""".r
    val pageSizeRegex = """"pageSize"\s*:\s*(\d+)""".r
    val page          = pageRegex.findFirstMatchIn(rawInput).map(m => m.group(1).toInt).getOrElse(0)
    val pageSize      = pageSizeRegex.findFirstMatchIn(rawInput).map(m => m.group(1).toInt).getOrElse(10)
    (page, pageSize)
  }

}
