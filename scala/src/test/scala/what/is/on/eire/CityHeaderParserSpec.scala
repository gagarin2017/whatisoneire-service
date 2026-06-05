package what.is.on.eire

import cats.effect.IO
import weaver.SimpleIOSuite

object CityHeaderParserSpec extends SimpleIOSuite {

  test("parse returns Some(city) when irish-location header is present") {
    val payload =
      """{
        |  "headers": { "irish-location": "Dublin" },
        |  "body": "{}"
        |}""".stripMargin

    IO(CityHeaderParser.parse(payload)).map { result =>
      expect(result == Some("Dublin"))
    }
  }

  test("parse returns Some(city) for a city with special characters") {
    val payload =
      """{
          |  "headers": { "irish-location": "Dún Laoghaire" },
          |  "body": "{}"
          |}""".stripMargin

    IO(CityHeaderParser.parse(payload)).map { result =>
      expect(result == Some("Dún Laoghaire"))
    }
  }

  test("parse returns None when irish-location header is absent") {
    val payload =
      """{
          |  "body": "{}"
          |}""".stripMargin

    IO(CityHeaderParser.parse(payload)).map { result =>
      expect(result == None)
    }
  }

  test("parse returns None for an empty payload") {
    val payload = "{}"

    IO(CityHeaderParser.parse(payload)).map { result =>
      expect(result == None)
    }
  }

}
