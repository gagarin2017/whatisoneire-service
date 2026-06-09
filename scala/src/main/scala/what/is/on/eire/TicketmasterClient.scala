package what.is.on.eire

import cats.Functor
import cats.syntax.functor._
import java.nio.charset.StandardCharsets
import smithy4s.Schema
import smithy4s.json.Json

class TicketmasterClient[F[_]: Functor](
  api: TicketmasterApi[F],
  apiKey: String
) {

  /** Fetch events for a specific Irish city. */
  def getEvents(city: String): F[List[IrishEvent]] =
    api
      .getTicketmasterEvents("IE", apiKey, Some(city))
      .map(toIrishEvents(_, Some(city)))

  /** Fetch ALL events across Ireland (no city filter).
    *
    * Used by the cron-triggered ingestion to get everything without knowing which cities exist.
    */
  def getAllEvents: F[List[IrishEvent]] =
    api
      .getTicketmasterEvents("IE", apiKey, None)
      .map(toIrishEvents(_, None))

  private def toIrishEvents(
    response: TicketmasterResponse,
    requestedCity: Option[String]
  ): List[IrishEvent] =
    response._embedded.events.flatMap(toIrishEvent(_, requestedCity))

  private def toIrishEvent(
    event: TicketmasterEvent,
    requestedCity: Option[String]
  ): Option[IrishEvent] = {
    val venue     = event._embedded.venues.flatMap(_.headOption)
    val venueCity = venue
      .flatMap(_.city)
      .flatMap(_.name)
      .orElse(requestedCity)
      .getOrElse("Unknown")
    val start     = event.dates.start
    val startDate = start.localDate.getOrElse("")
    val startTime = start.localTime

    Some(
      IrishEvent(
        id = event.id,
        title = event.name,
        url = event.url.getOrElse(""),
        startDate = startDate,
        startTime = startTime,
        city = venueCity,
        county = toCounty(venueCity),
        coordinates = toCoordinates(venue),
        source = "Ticketmaster",
        rawPayload = serializeEvent(event)
      )
    )
  }

  /** Serialize the full [[TicketmasterEvent]] to a JSON string for storage as rawPayload. */
  private def serializeEvent(event: TicketmasterEvent): String = {
    val encoder = Json.payloadCodecs.encoders.fromSchema(Schema[TicketmasterEvent])
    val blob    = encoder.encode(event)
    new String(blob.toArray, StandardCharsets.UTF_8)
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
