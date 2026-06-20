package what.is.on.eire

import cats.effect.IO
import weaver.SimpleIOSuite

/** Tests for [[GetEventsLambdaHandler.parsePagination]].
  *
  * The parser must handle both escaped quotes (`\"page\"`) and plain quotes (`"page"`) because the
  * Lambda payload's `body` field may be serialized either way depending on the API Gateway / local
  * test runner.
  */
object GetEventsLambdaHandlerSpec extends SimpleIOSuite {

  // Access the private method via reflection
  private val handler = new GetEventsLambdaHandler {}

  private def parsePagination(rawInput: String): (Int, Int) = {
    val method =
      classOf[GetEventsLambdaHandler].getDeclaredMethod("parsePagination", classOf[String])
    method.setAccessible(true)
    method.invoke(handler, rawInput).asInstanceOf[(Int, Int)]
  }

  test("extracts page and pageSize from plain-quoted body") {
    IO.pure {
      val payload          =
        """{
          |  "headers": {},
          |  "body": "{ "page": 2, "pageSize": 10 }"
          |}""".stripMargin
      val (page, pageSize) = parsePagination(payload)
      expect.all(page == 2, pageSize == 10)
    }
  }

  test("extracts page and pageSize from backslash-escaped body") {
    IO.pure {
      val payload          =
        """{
          |  "headers": {},
          |  "body": "{\"page\": 3, \"pageSize\": 20}"
          |}""".stripMargin
      val (page, pageSize) = parsePagination(payload)
      expect.all(page == 3, pageSize == 20)
    }
  }

  test("extracts page 0 when the body page value is 0") {
    IO.pure {
      val payload   =
        """{
          |  "headers": {},
          |  "body": "{\"page\": 0, \"pageSize\": 10}"
          |}""".stripMargin
      val (page, _) = parsePagination(payload)
      expect(page == 0)
    }
  }

  test("defaults to page=0, pageSize=10 when no pagination is present") {
    IO.pure {
      val payload          =
        """{
          |  "headers": {},
          |  "body": "{}"
          |}""".stripMargin
      val (page, pageSize) = parsePagination(payload)
      expect.all(page == 0, pageSize == 10)
    }
  }

  test("extracts page from the body value, not a phantom outer field") {
    IO.pure {
      val payload          =
        """{
          |  "headers": { "page": "irrelevant" },
          |  "body": "{ \"page\": 5, \"pageSize\": 15 }"
          |}""".stripMargin
      val (page, pageSize) = parsePagination(payload)
      expect.all(page == 5, pageSize == 15)
    }
  }

  test("matches the exact format seen in the user's bug-report logs") {
    IO.pure {
      val payload          =
        """{
          |  "headers": {  },
          |  "body": "{ "page": 2, "pageSize": 10 }"
          |}""".stripMargin
      val (page, pageSize) = parsePagination(payload)
      expect.all(page == 2, pageSize == 10)
    }
  }

  test("matches the HttpLambdaRunner format (String interpolation escaping)") {
    IO.pure {
      val payload          =
        """{
          |  "headers": {  },
          |  "body": "{ \"page\": 1, \"pageSize\": 25 }"
          |}""".stripMargin
      val (page, pageSize) = parsePagination(payload)
      expect.all(page == 1, pageSize == 25)
    }
  }

  test("returns defaults for empty body string") {
    IO.pure {
      val payload          =
        """{
          |  "headers": {},
          |  "body": ""
          |}""".stripMargin
      val (page, pageSize) = parsePagination(payload)
      expect.all(page == 0, pageSize == 10)
    }
  }

}
