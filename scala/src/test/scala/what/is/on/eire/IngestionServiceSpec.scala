package what.is.on.eire

import cats.effect.IO
import weaver.SimpleIOSuite

object IngestionServiceSpec extends SimpleIOSuite {

  private val sampleEvent = IrishEvent(
    id = "Z7r9jZ1AdOkT9",
    title = "Test Event",
    url = "https://www.ticketmaster.ie/test",
    startDate = "2026-06-15",
    city = "Dublin",
    county = IrishCounty.DUBLIN,
    source = "Ticketmaster",
    startTime = None,
    coordinates = None
  )

  private val sampleEvent1 = IrishEvent(
    id = "Z7r9jZ1AdOkT9",
    title = "Test Event 1",
    url = "https://www.ticketmaster.ie/test",
    startDate = "2026-06-15",
    city = "Dublin",
    county = IrishCounty.DUBLIN,
    source = "Ticketmaster",
    startTime = None,
    coordinates = None
  )

  test("fetchEvents calls fetchByCity when irish-location header is present") {
    val payload =
      """{
          |  "headers": { "irish-location": "Cork" },
          |  "body": "{}"
          |}""".stripMargin

    val service = new IngestionService[IO](
      fetchByCity = city => IO.pure(List(sampleEvent.copy(city = city))),
      fetchAll = IO.pure(List(sampleEvent))
    )

    for {
      result <- service.fetchEvents(payload)
    } yield expect.all(
      result.size == 1,
      result.head.city == "Cork"
    )
  }

  test("fetchEvents calls fetchAll when irish-location header is absent") {
    val payload =
      """{
            |  "body": "{}"
            |}""".stripMargin

    val service = new IngestionService[IO](
      fetchByCity = city => IO.pure(List(sampleEvent.copy(city = city))),
      fetchAll = IO.pure(List(sampleEvent))
    )

    for {
      result <- service.fetchEvents(payload)
    } yield expect.all(
      result.size == 1,
      result.head.city == "Dublin"
    )

  }

  test("fetchEvents calls fetchAll for an empty payload") {
    val payload = "{}"

    val service = new IngestionService[IO](
      fetchByCity = city => IO.pure(List(sampleEvent.copy(city = city))),
      fetchAll = IO.pure(List(sampleEvent, sampleEvent1))
    )

    for {
      result     <- service.fetchEvents(payload)
      firstEvent  = result(0)
      secondEvent = result(1)
    } yield expect.all(
      result.size == 2,
      firstEvent.city == "Dublin",
      secondEvent.title == "Test Event 1"
    )
  }

}
