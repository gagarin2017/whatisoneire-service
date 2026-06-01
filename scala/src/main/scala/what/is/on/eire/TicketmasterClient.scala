package what.is.on.eire

import cats.effect.Concurrent
import cats.syntax.all._
import org.http4s.MediaType
import org.http4s.Method
import org.http4s.Request
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.headers.Accept
import smithy4s.Blob
import smithy4s.json.Json

class TicketmasterClient[F[_]: Concurrent](
  client: Client[F],
  baseUri: Uri,
  apiKey: String
) {

  private val responseDecoder =
    Json.payloadCodecs.decoders.fromSchema(TicketmasterResponse.schema)

  def getEvents(city: String): F[List[IrishEvent]] = {
    val uri = baseUri
      .withQueryParam("countryCode", "IE")
      .withQueryParam("city", city)
      .withQueryParam("apikey", apiKey)

    val request = Request[F](Method.GET, uri)
      .withHeaders(Accept(MediaType.application.json))

    client.expect[Array[Byte]](request).flatMap { bytes =>
      decode(bytes).map(toIrishEvents(_, city))
    }
  }

  private def decode(bytes: Array[Byte]): F[TicketmasterResponse] =
    Concurrent[F].fromEither(
      responseDecoder
        .decode(Blob(bytes))
        .leftMap(error => new RuntimeException(error.toString))
    )

  private def toIrishEvents(
    response: TicketmasterResponse,
    requestedCity: String
  ): List[IrishEvent] =
    response._embedded
      .flatMap(_.events)
      .getOrElse(Nil)
      .flatMap(toIrishEvent(_, requestedCity))

  private def toIrishEvent(
    event: TicketmasterEvent,
    requestedCity: String
  ): Option[IrishEvent] = {
    val venue       = event._embedded.flatMap(_.venues).flatMap(_.headOption)
    val venueCity   =
      venue.flatMap(_.city).flatMap(_.name).getOrElse(requestedCity)
    val coordinates = toCoordinates(venue)

    for {
      dates <- event.dates
      start <- dates.start
    } yield IrishEvent(
      id = event.id,
      title = event.name,
      url = event.url,
      startDate = start.localDate,
      startTime = start.localTime,
      city = venueCity,
      county = toCounty(venueCity),
      coordinates = coordinates,
      source = "Ticketmaster"
    )
  }

  private def toCoordinates(
    venue: Option[TicketmasterVenue]
  ): Option[GeoCoordinates] =
    for {
      actualVenue <- venue
      location    <- actualVenue.location
      latitude    <- location.latitude.flatMap(_.toDoubleOption)
      longitude   <- location.longitude.flatMap(_.toDoubleOption)
    } yield GeoCoordinates(latitude, longitude)

  private def toCounty(city: String): IrishCounty =
    city.trim.toLowerCase match {
      case "dublin" | "co. dublin" => IrishCounty.DUBLIN
      case "galway"                => IrishCounty.GALWAY
      case "cork"                  => IrishCounty.CORK
      case "meath"                 => IrishCounty.MEATH
      case _                       => IrishCounty.UNKNOWN
    }
}
