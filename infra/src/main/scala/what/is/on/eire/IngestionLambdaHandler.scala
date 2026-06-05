package what.is.on.eire

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets

class IngestionLambdaHandler extends RequestStreamHandler {

  private given runtime: IORuntime = IORuntime.global

  private val apiKey: String =
    Option(System.getenv("TICKETMASTER_API_KEY"))
      .getOrElse(throw new RuntimeException("TICKETMASTER_API_KEY env var not set"))

  override def handleRequest(
    input: InputStream,
    output: OutputStream,
    context: Context
  ): Unit = {
    val logger = context.getLogger
    logger.log("WhatsOnEire Ingestion Lambda invoked!")

    val rawInput = new String(input.readAllBytes(), StandardCharsets.UTF_8)
    logger.log(s"Incoming event payload (first 200 chars): ${rawInput.take(200)}")

    // ── Parse the irish-location header from the incoming payload ──
    // Supports both: { "headers": { "irish-location": "Dublin" } }
    // and:           { "headers": { "irish-location": "Dublin" } }
    val requestedCity: Option[String] = {
      val headerRegex = """irish-location"\s*:\s*"([^"]+)"""".r
      rawInput match {
        case headerRegex(city) => Some(city)
        case _                 => None // cron trigger — no location
      }
    }
    logger.log(s"Requested city: ${requestedCity.getOrElse("ALL (cron)")}")

    // ── Build http4s client ──────────────────────────────────────────
    val httpClient
      : cats.effect.kernel.Resource[cats.effect.IO, org.http4s.client.Client[cats.effect.IO]] =
      org.http4s.ember.client.EmberClientBuilder
        .default[cats.effect.IO]
        .build

    // ── Extract client & build TicketmasterApi in the for-comp ───────
    val program = for {
      client    <- httpClient
      tmApi     <- smithy4s.http4s
                     .SimpleRestJsonBuilder(TicketmasterApi)
                     .client(client)
                     .uri(org.http4s.Uri.unsafeFromString("https://app.ticketmaster.com"))
                     .resource
      publisher <- {
        val localstackPort =
          Option(System.getenv("LOCALSTACK_PORT")).flatMap(p => scala.util.Try(p.toInt).toOption)
        KinesisEventPublisher.resource("events-raw-stream", localstackPort)
      }
    } yield {
      val tmClient = new TicketmasterClient[cats.effect.IO](tmApi, apiKey)

      // Fetch events — filtered by city if requested, otherwise all
      val fetch = requestedCity match {
        case Some(city) => tmClient.getEvents(city)
        case None       => tmClient.getAllEvents
      }

      val events = fetch.unsafeRunSync()
      val label  = requestedCity.getOrElse("all Ireland")
      logger.log(s"Fetched ${events.size} events from Ticketmaster for $label")

      events.foreach { e =>
        publisher.publish(e).unsafeRunSync()
        logger.log(s"  Published: ${e.id} - ${e.title} @ ${e.city}, ${e.county}")
      }

      events // return so the response includes the count
    }

    try {
      val events = program.use(evts => IO.pure(evts)).unsafeRunSync()
      val result =
        s"""{"status": "SUCCESS", "count": ${events.size}, "message": "Published ${events.size} events to Kinesis"}"""
      output.write(result.getBytes(StandardCharsets.UTF_8))
    } catch {
      case e: Exception =>
        logger.log(s"ERROR: ${e.getMessage}")
        val error = s"""{"status": "FAILED", "message": "${e.getMessage}"}"""
        output.write(error.getBytes(StandardCharsets.UTF_8))
    }
  }
}
