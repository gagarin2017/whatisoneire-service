package what.is.on.eire

import cats.MonadThrow
import cats.effect.Sync
import cats.syntax.functor._

class MainService[F[_]: MonadThrow](ticketmasterClient: TicketmasterClient[F])
    extends WhatIsOnEireApi[F] {

  private val sampleCoordinates = GeoCoordinates(53.3498, -6.2603)

  private val myEventDummy = IrishEvent(
    id = "1A0Zkv4Gkd97a",
    title = "Live Traditional Session",
    url = "https://ticketmaster.ie/...",
    startDate = "2026-08-15",
    startTime = Some("20:00:00"),
    city = "Dublin",
    county = IrishCounty.DUBLIN,
    coordinates = Some(sampleCoordinates),
    source = "Ticketmaster",
    rawPayload = "{}"
  )

  override def pullEvents(location: String): F[PullEventsOutput] =
    ticketmasterClient
      .getEvents(location)
      .map(events => PullEventsOutput(events))

}
