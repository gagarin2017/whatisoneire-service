package what.is.on.eire

import cats.effect.IO
import weaver.SimpleIOSuite

object EventStoreServiceSpec extends SimpleIOSuite {

  private val validJson: String =
    """{
      |  "id": "EVT001",
      |  "title": "Test Event",
      |  "url": "https://ticketmaster.ie/test",
      |  "startDate": "2026-06-15",
      |  "city": "Dublin",
      |  "county": "IE-D",
      |  "source": "Ticketmaster",
      |  "rawPayload": "{}"
      |}""".stripMargin

  private val anotherValidJson: String =
    """{
      |  "id": "EVT002",
      |  "title": "Cork Festival",
      |  "url": "https://ticketmaster.ie/cork",
      |  "startDate": "2026-07-01",
      |  "city": "Cork",
      |  "county": "IE-C",
      |  "source": "Ticketmaster",
      |  "rawPayload": "{}"
      |}""".stripMargin

  test("processBatch saves all valid records and reports correct count") {
    var savedCount = 0

    val service = new EventStoreService[IO](
      saveEvent = event =>
        IO {
          savedCount += 1
          s"${event.id} - ${event.title}"
        }
    )

    service.processBatch(List(validJson, anotherValidJson)).map { result =>
      expect.all(
        result.contains("2 saved"),
        result.contains("0 skipped"),
        savedCount == 2
      )
    }
  }

  test("processBatch saves valid records and skips invalid ones") {
    val invalidJson = "{}"

    val service = new EventStoreService[IO](
      saveEvent = event => IO.pure(s"${event.id} saved")
    )

    service.processBatch(List(validJson, invalidJson, anotherValidJson)).map { result =>
      expect.all(
        result.contains("2 saved"),
        result.contains("1 skipped")
      )
    }
  }

  test("processBatch reports 0 saved when all records are invalid") {
    val invalid1 = "{}"
    val invalid2 = """{"bad": "data"}"""

    val service = new EventStoreService[IO](
      saveEvent = event => IO.pure(s"${event.id} saved")
    )

    service.processBatch(List(invalid1, invalid2)).map { result =>
      expect.all(
        result.contains("0 saved"),
        result.contains("2 skipped")
      )
    }
  }

}
