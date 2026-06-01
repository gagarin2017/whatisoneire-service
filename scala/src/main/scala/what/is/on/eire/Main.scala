package what.is.on.eire

// replace the old imports with this single line
import cats.effect.IO
import cats.effect.IOApp
import java.nio.file.Files
import java.nio.file.Paths
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits._
import smithy4s.http4s.SimpleRestJsonBuilder

// change the entry point from App to IOApp.Simple
object Main extends IOApp.Simple {

  private val ticketmasterUri =
    uri"https://app.ticketmaster.com/discovery/v2/events.json"

  private val apiKey =
    readDotEnvValue("http/.env", "TICKETMASTER_API_KEY")

  private def readDotEnvValue(path: String, key: String): String = {
    val envPath = Paths.get(path)

    if (!Files.exists(envPath)) {
      throw new RuntimeException(s"Missing $path file")
    }

    Files
      .readAllLines(envPath)
      .toArray
      .toList
      .collectFirst {
        case line: String if line.trim.startsWith(s"$key=") =>
          line.substring(line.indexOf("=") + 1).trim
      }
      .filter(_.nonEmpty)
      .getOrElse(
        throw new RuntimeException(s"Missing $key value in $path")
      )
  }

  val sampleCoordinates = GeoCoordinates(53.3498, -6.2603)

  val myEvent = IrishEvent(
    id = "1A0Zkv4Gkd97a",
    title = "Live Traditional Session",
    url = "https://ticketmaster.ie/…",
    startDate = "2026-08-15",
    startTime = Some("20:00:00"),
    city = "Dublin",
    county = IrishCounty.DUBLIN,
    coordinates = Some(sampleCoordinates),
    source = "Ticketmaster"
  )

  println(
    s"Successfully initialized Smithy record for: ${myEvent.title} in county ${myEvent.county.value}"
  )
  override def run: IO[Unit] =
    EmberClientBuilder.default[IO].build.use { httpClient =>
      val ticketmasterClient =
        new TicketmasterClient[IO](httpClient, ticketmasterUri, apiKey)

      SimpleRestJsonBuilder
        .routes(new MainService[IO](ticketmasterClient))
        .resource
        .flatMap(routes =>
          EmberServerBuilder
            .default[IO]
            .withHttpApp(routes.orNotFound)
            .build
        )
        .useForever
    }
}
