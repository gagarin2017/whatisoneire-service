package what.is.on.eire

import org.http4s.ember.server.EmberServerBuilder
// replace the old imports with this single line
import cats.effect.{IO, IOApp}
import smithy4s.http4s.SimpleRestJsonBuilder
import org.http4s.implicits._

// change the entry point from App to IOApp.Simple
object Main extends IOApp.Simple {
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

  // replace the three‑line server block with a single `run` definition
  override def run: IO[Unit] =
    SimpleRestJsonBuilder
      .routes(new MainService[IO])
      .resource
      .flatMap(routes =>
        EmberServerBuilder
          .default[IO]
          .withHttpApp(
            routes.orNotFound
          ) // <- `orNotFound` now lives on HttpRoutes
          .build
      )
      .useForever
}
