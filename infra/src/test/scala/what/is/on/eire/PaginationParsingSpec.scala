package what.is.on.eire

import cats.effect.IO
import weaver.SimpleIOSuite

object PaginationParsingSpec extends SimpleIOSuite {

  // Exact copy of parsePagination from GetEventsLambdaHandler
  private def parsePagination(rawInput: String): (Int, Int) = {
    val pageRegex     = """"page"\s*:\s*(\d+)""".r
    val pageSizeRegex = """"pageSize"\s*:\s*(\d+)""".r
    val page          = pageRegex.findFirstMatchIn(rawInput).map(m => m.group(1).toInt).getOrElse(0)
    val pageSize      = pageSizeRegex.findFirstMatchIn(rawInput).map(m => m.group(1).toInt).getOrElse(10)
    (page, pageSize)
  }

  // Exact payload built by HttpLambdaRunner.invokeReadLambda when page=1, pageSize=10, city=None
  private val payloadFromHttpRunner =
    """{
      |  "headers": {  },
      |  "page": 1,
      |  "pageSize": 10
      |}""".stripMargin

  test("parsePagination extracts page=1 from HttpLambdaRunner payload") {
    IO {
      val (page, pageSize) = parsePagination(payloadFromHttpRunner)
      expect.all(page == 1, pageSize == 10)
    }
  }

  test("parsePagination extracts page=0 from HttpLambdaRunner payload") {
    IO {
      val payload          =
        """{
          |  "headers": {  },
          |  "page": 0,
          |  "pageSize": 10
          |}""".stripMargin
      val (page, pageSize) = parsePagination(payload)
      expect.all(page == 0, pageSize == 10)
    }
  }

  test("parsePagination with page first works") {
    IO {
      val payload          = """{"page": 2, "pageSize": 20}"""
      val (page, pageSize) = parsePagination(payload)
      expect.all(page == 2, pageSize == 20)
    }
  }

  test("parsePagination with pageSize before page still works") {
    IO {
      val payload          = """{"pageSize": 30, "page": 3}"""
      val (page, pageSize) = parsePagination(payload)
      expect.all(page == 3, pageSize == 30)
    }
  }

  test("parsePagination defaults when fields missing") {
    IO {
      val (page, pageSize) = parsePagination("""{"headers": {}}""")
      expect.all(page == 0, pageSize == 10)
    }
  }

  // Raw regex test to rule out Regex.unapplySeq issues
  test("raw regex findFirstMatchIn works on payload where pageSize comes first") {
    IO {
      val payload   = """{"pageSize": 30, "page": 3}"""
      val pageRegex = """"page"\s*:\s*(\d+)""".r

      println(s"Input: $payload")
      println(s"Regex: $pageRegex")

      val match1 = pageRegex.findFirstMatchIn(payload)
      println(s"findFirstMatchIn result: $match1")
      println(s"group(1): ${match1.map(_.group(1))}")

      // Also test with unapplySeq directly
      val unapplyResult = pageRegex.unapplySeq(payload)
      println(s"unapplySeq result: $unapplyResult")

      val (page, _) = parsePagination(payload)
      println(s"parsePagination page: $page")

      expect.all(
        match1.map(_.group(1)) == Some("3"),
        page == 3
      )
    }
  }
}
