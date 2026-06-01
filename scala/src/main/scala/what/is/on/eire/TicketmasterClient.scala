package what.is.on.eire

import cats.Functor
import cats.syntax.functor._

class TicketmasterClient[F[_]: Functor](
  api: TicketmasterApi[F],
  apiKey: String
) {

  def getEvents(city: String): F[List[IrishEvent]] =
    api
      .getTicketmasterEvents("IE", city, apiKey)
      .map(toIrishEvents(_, city))

  private def toIrishEvents(
    response: TicketmasterResponse,
    requestedCity: String
  ): List[IrishEvent] =
    response._embedded.events.flatMap(toIrishEvent(_, requestedCity))

  private def toIrishEvent(
    event: TicketmasterEvent,
    requestedCity: String
  ): Option[IrishEvent] = {
    val venue     = event._embedded.venues.headOption
    val venueCity = venue
      .flatMap(_.city)
      .flatMap(_.name)
      .getOrElse(requestedCity)

    Some(
      IrishEvent(
        id = event.id,
        title = event.name,
        url = event.url,
        startDate = event.dates.start.localDate,
        startTime = event.dates.start.localTime,
        city = venueCity,
        county = toCounty(venueCity),
        coordinates = toCoordinates(venue),
        source = "Ticketmaster"
      )
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
