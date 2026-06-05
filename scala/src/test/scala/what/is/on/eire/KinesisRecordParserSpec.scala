package what.is.on.eire

import cats.effect.IO
import weaver.SimpleIOSuite

object KinesisRecordParserSpec extends SimpleIOSuite {

  test("parse returns an IrishEvent for valid JSON with all fields") {
    val json =
      """{
        |  "id": "Z7r9jZ1AdOkT9",
        |  "title": "Live Traditional Session",
        |  "url": "https://ticketmaster.ie/test",
        |  "startDate": "2026-08-15",
        |  "startTime": "20:00:00",
        |  "city": "Dublin",
        |  "county": "DUBLIN",
        |  "coordinates": { "latitude": 53.3498, "longitude": -6.2603 },
        |  "source": "Ticketmaster"
        |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect.all(
        result.isDefined,
        result.get.id == "Z7r9jZ1AdOkT9",
        result.get.title == "Live Traditional Session",
        result.get.city == "Dublin",
        result.get.county == IrishCounty.DUBLIN,
        result.get.startTime == Some("20:00:00"),
        result.get.coordinates.isDefined,
        result.get.source == "Ticketmaster"
      )
    }
  }

  test("parse succeeds when startTime is absent (optional field — defaults to None)") {
    val json =
      """{
          |  "id": "EVT001",
          |  "title": "Galway Arts Festival",
          |  "url": "https://ticketmaster.ie/galway",
          |  "startDate": "2026-07-20",
          |  "city": "Galway",
          |  "county": "GALWAY",
          |  "source": "Ticketmaster"
          |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect.all(
        result.isDefined,
        result.get.startTime == None,
        result.get.county == IrishCounty.GALWAY
      )
    }
  }

  test("parse succeeds when coordinates are absent (optional field)") {
    val json =
      """{
          |  "id": "EVT002",
          |  "title": "Cork Jazz Weekend",
          |  "url": "https://ticketmaster.ie/cork",
          |  "startDate": "2026-10-25",
          |  "city": "Cork",
          |  "county": "CORK",
          |  "source": "Ticketmaster"
          |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect.all(
        result.isDefined,
        result.get.coordinates == None,
        result.get.county == IrishCounty.CORK
      )
    }
  }

  test("parse returns None when a hard-required field (id) is missing") {
    val json =
      """{
          |  "title": "No ID Event",
          |  "city": "Dublin",
          |  "county": "DUBLIN",
          |  "source": "Ticketmaster"
          |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect(result == None)
    }
  }

  test("parse succeeds when url is missing (optional field — defaults to empty string)") {
    val json =
      """{
          |  "id": "EVT100",
          |  "title": "No URL Event",
          |  "startDate": "2026-06-01",
          |  "city": "Dublin",
          |  "county": "DUBLIN",
          |  "source": "Ticketmaster"
          |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect.all(
        result.isDefined,
        result.get.url == ""
      )
    }
  }

  test("parse succeeds when startDate is missing (optional field — defaults to empty string)") {
    val json =
      """{
          |  "id": "EVT101",
          |  "title": "No Date Event",
          |  "url": "https://ticketmaster.ie/no-date",
          |  "city": "Galway",
          |  "county": "GALWAY",
          |  "source": "Ticketmaster"
          |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect.all(
        result.isDefined,
        result.get.startDate == ""
      )
    }
  }

  test("parse succeeds with UNKNOWN county when county is not a recognised IrishCounty") {
    val json =
      """{
          |  "id": "EVT003",
          |  "title": "Mystery Event",
          |  "url": "https://ticketmaster.ie/mystery",
          |  "startDate": "2026-06-01",
          |  "city": "Nowhere",
          |  "county": "NARNIA",
          |  "source": "Ticketmaster"
          |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect.all(
        result.isDefined,
        result.get.county == IrishCounty.UNKNOWN
      )
    }
  }

  test("parse returns None for an empty JSON object") {
    val json = "{}"

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect(result == None)
    }
  }

}
