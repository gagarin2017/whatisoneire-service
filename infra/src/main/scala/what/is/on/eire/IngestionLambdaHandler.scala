package what.is.on.eire

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.IORuntime
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

class IngestionLambdaHandler extends RequestStreamHandler {

  private given runtime: IORuntime = IORuntime.global

  private val apiKey: String = EnvLoader.require("TICKETMASTER_API_KEY")

  /** Number of months ahead from today to pull events for. Configured via `EVENTS_LOOKAHEAD_MONTHS`
    * in the `.env` file or as an environment variable.
    */
  private val lookAheadMonths: Int =
    EnvLoader
      .require("EVENTS_LOOKAHEAD_MONTHS")
      .toInt

  override def handleRequest(
    input: InputStream,
    output: OutputStream,
    context: Context
  ): Unit = {
    val logger = context.getLogger
    logger.log("WhatsOnEire Ingestion Lambda invoked!")

    val rawInput = new String(input.readAllBytes(), StandardCharsets.UTF_8)
    logger.log(s"Incoming event payload: $rawInput")

    // ── Build http4s client ──────────────────────────────────────────
    val httpClient: Resource[cats.effect.IO, Client[cats.effect.IO]] =
      EmberClientBuilder.default[cats.effect.IO].build

    // ── Build the program as a Resource ─────────────────────────────
    val program = for {
      client    <- httpClient
      tmApi     <- smithy4s.http4s
                     .SimpleRestJsonBuilder(TicketmasterApi)
                     .client(client)
                     .uri(org.http4s.Uri.unsafeFromString("https://app.ticketmaster.com"))
                     .resource
      publisher <- {
        val localstackPort =
          EnvLoader.get("LOCALSTACK_PORT").flatMap(p => scala.util.Try(p.toInt).toOption)
        KinesisEventPublisher.resource("events-raw-stream", localstackPort)
      }
    } yield {
      val tmClient = new TicketmasterClient[cats.effect.IO](tmApi, apiKey, lookAheadMonths)

      val service = new IngestionService[IO](
        fetchByCity = city => tmClient.getEvents(city),
        fetchAll = tmClient.getAllEvents
      )

      logger.log("Calling Ticketmaster API to fetch events...")
      val events = service.fetchEvents(rawInput).unsafeRunSync()
      logger.log(s"Fetched ${events.size} events from Ticketmaster")

      events.foreach { e =>
        publisher.publish(e).unsafeRunSync()
        logger.log(s"  Published: ${e.id} - ${e.title} @ ${e.city}, ${e.county}")
      }

      events
    }

    try {
      val events = program.use(evts => IO.pure(evts)).unsafeRunSync()
      val result =
        s"""{"status": "SUCCESS", "count": ${events.size}, "message": "Published ${events.size} events to Kinesis"}"""
      output.write(result.getBytes(StandardCharsets.UTF_8))
    } catch {
      case e: Throwable =>
        logger.log(s"ERROR: ${e.getMessage}\n${e.getStackTrace.map(_.toString).mkString("\n")}")
        val error = s"""{"status": "FAILED", "message": "${e.getMessage}"}"""
        output.write(error.getBytes(StandardCharsets.UTF_8))
    }
  }
}
