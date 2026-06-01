package what.is.on.eire

import cats.effect.Sync

class MainService[F[_]: Sync] extends WhatIsOnEireApi[F] {

  private val sampleCoordinates = GeoCoordinates(53.3498, -6.2603)

  private val myEvent = IrishEvent(
    id = "1A0Zkv4Gkd97a",
    title = "Live Traditional Session",
    url = "https://ticketmaster.ie/...",
    startDate = "2026-08-15",
    startTime = Some("20:00:00"),
    city = "Dublin",
    county = IrishCounty.DUBLIN,
    coordinates = Some(sampleCoordinates),
    source = "Ticketmaster"
  )

  override def pullEvents(
      location: String
  ): F[PullEventsOutput] =
    Sync[F].pure(PullEventsOutput(List(myEvent)))

}
