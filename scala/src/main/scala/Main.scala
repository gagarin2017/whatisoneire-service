import what.is.on.eire.*

object Main extends App {
  val sampleCoordinates = GeoCoordinates(53.3498, -6.2603)
  
  val myEvent = IrishEvent(
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

  println(s"Successfully initialized Smithy record for: ${myEvent.title} in county ${myEvent.county.value}")
}