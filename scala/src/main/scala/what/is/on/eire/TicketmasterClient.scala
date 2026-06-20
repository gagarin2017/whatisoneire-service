package what.is.on.eire

import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._
import java.nio.charset.StandardCharsets
import smithy4s.Schema
import smithy4s.json.Json

class TicketmasterClient[F[_]: Monad](
  api: TicketmasterApi[F],
  apiKey: String
) {

  /** Fetch events for a specific Irish city. */
  def getEvents(city: String): F[List[IrishEvent]] =
    api
      .getTicketmasterEvents("IE", apiKey, Some(city), None, None)
      .map(toIrishEvents(_, Some(city)))

  /** Maximum results per page accepted by the Ticketmaster Discovery API.
    *
    * The API rejects `size >= 200` with error code DIS1036. Using the max (199) minimises the
    * number of round-trips when paginating through all events.
    */
  private val maxPageSize: Int = 199

  /** Maximum paging offset (page * size) allowed by the Ticketmaster Discovery API.
    *
    * The API rejects requests where `(page * size) >= 1000` with error code DIS1035. This means we
    * can fetch at most 999 items from the start (e.g. pages 0-5 with size=199). The API's
    * `resp.page.totalPages` can be misleading because it's sometimes computed at the default page
    * size of 20 rather than the requested size, so we enforce this limit as a hard safety check.
    */
  private val maxPagingDepth: Int = 999

  /** Fetch ALL events across every Irish city by paginating through every page.
    *
    * Starts with page 0, reads `totalPages` from the response, then fetches remaining pages
    * sequentially. Uses the maximum allowed page size to reduce the number of HTTP round-trips.
    */
  def getAllEvents: F[List[IrishEvent]] =
    fetchAllPages(maxPageSize, 0, Nil)

  private def fetchAllPages(
    size: Int,
    page: Int,
    acc: List[IrishEvent]
  ): F[List[IrishEvent]] = {
    val response = api.getTicketmasterEvents("IE", apiKey, None, Some(size), Some(page))
    response.flatMap { resp =>
      val events  = acc ++ toIrishEvents(resp, None)
      val totalPg = resp.page.totalPages
      val nextPg  = page + 1
      if (nextPg < totalPg && nextPg * size <= maxPagingDepth) fetchAllPages(size, nextPg, events)
      else Monad[F].pure(events)
    }
  }

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
