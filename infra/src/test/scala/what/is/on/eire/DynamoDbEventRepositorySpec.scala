package what.is.on.eire

import cats.effect.IO
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import scala.jdk.CollectionConverters._
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import weaver.SimpleIOSuite

object DynamoDbEventRepositorySpec extends SimpleIOSuite {

  private val tableName = "test-events"

  /** Build a DynamoDB item map that looks like a stored [[IrishEvent]]. */
  private def eventItem(id: Int): java.util.Map[String, AttributeValue] = {
    val padded = f"$id%04d"
    val item   = new java.util.HashMap[String, AttributeValue]()
    item.put("id", AttributeValue.builder().s(padded).build())
    item.put("title", AttributeValue.builder().s(s"Event $padded").build())
    item.put("url", AttributeValue.builder().s(s"https://example.com/$padded").build())
    item.put("startDate", AttributeValue.builder().s("2026-06-01").build())
    item.put("city", AttributeValue.builder().s("Dublin").build())
    item.put("county", AttributeValue.builder().s("IE-D").build())
    item.put("source", AttributeValue.builder().s("Test").build())
    item.put("rawPayload", AttributeValue.builder().s("{}").build())
    item
  }

  /** Create a mock [[DynamoDbClient]] whose `scan()` respects `.limit()`.
    *
    * This is critical — the mock replicates real DynamoDB behaviour: when `.limit(n)` is set on the
    * `ScanRequest`, only the first `n` items are returned. Without `.limit()`, all items are
    * returned. This lets the test catch the pagination bug where `.limit(pageSize)` was applied
    * (limiting the scan to `pageSize` items so pages > 0 were always empty).
    */
  private def mockClient(allItems: List[java.util.Map[String, AttributeValue]]): DynamoDbClient = {
    val handler = new InvocationHandler {
      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
        method.getName match {
          case "scan"     =>
            val request      = args(0).asInstanceOf[ScanRequest]
            val isCountQuery = Option(request.select()).exists(
              _ == software.amazon.awssdk.services.dynamodb.model.Select.COUNT
            )
            if (isCountQuery) {
              ScanResponse.builder.count(allItems.size).build()
            } else {
              val limitOption  = Option(request.limit()).map(_.intValue()).filter(_ > 0)
              val limit        = limitOption.getOrElse(allItems.size)
              val limitedItems = allItems.take(limit)
              ScanResponse.builder
                .items(limitedItems.asJava)
                .count(limitedItems.size)
                .scannedCount(limitedItems.size)
                .build()
            }
          case "close"    => null
          case "toString" => s"MockDynamoDbClient(${allItems.size} items)"
          case "hashCode" => System.identityHashCode(proxy).asInstanceOf[AnyRef]
          case "equals"   => (proxy.asInstanceOf[AnyRef] eq args(0)).asInstanceOf[AnyRef]
          case name       =>
            throw new UnsupportedOperationException(s"Unexpected DynamoDbClient call: $name")
        }
      }
    }

    Proxy
      .newProxyInstance(
        classOf[DynamoDbClient].getClassLoader,
        Array(classOf[DynamoDbClient]),
        handler
      )
      .asInstanceOf[DynamoDbClient]
  }

  // ── Page-by-page tests ──────────────────────────────────────────────────

  test("getEventsPaginated page 0 returns the first pageSize events") {
    val allItems = (1 to 25).map(eventItem).toList
    val repo     = new DynamoDbEventRepository(mockClient(allItems), tableName)

    repo.getEventsPaginated(None, 0, 10).map { case (events, total) =>
      expect.all(
        events.size == 10,
        total == 25,
        events.map(_.id) == (1 to 10).map(i => f"$i%04d").toList
      )
    }
  }

  test("getEventsPaginated page 1 returns the next pageSize events") {
    val allItems = (1 to 25).map(eventItem).toList
    val repo     = new DynamoDbEventRepository(mockClient(allItems), tableName)

    repo.getEventsPaginated(None, 1, 10).map { case (events, total) =>
      expect.all(
        events.size == 10,
        total == 25,
        events.map(_.id) == (11 to 20).map(i => f"$i%04d").toList
      )
    }
  }

  test("getEventsPaginated page 2 returns the remaining events (last page)") {
    val allItems = (1 to 25).map(eventItem).toList
    val repo     = new DynamoDbEventRepository(mockClient(allItems), tableName)

    repo.getEventsPaginated(None, 2, 10).map { case (events, total) =>
      expect.all(
        events.size == 5,
        total == 25,
        events.map(_.id) == (21 to 25).map(i => f"$i%04d").toList
      )
    }
  }

  test("getEventsPaginated returns different event IDs for each page") {
    val allItems = (1 to 25).map(eventItem).toList
    val repo     = new DynamoDbEventRepository(mockClient(allItems), tableName)

    for {
      page0 <- repo.getEventsPaginated(None, 0, 10)
      page1 <- repo.getEventsPaginated(None, 1, 10)
      page2 <- repo.getEventsPaginated(None, 2, 10)
    } yield {
      val (events0, _) = page0
      val (events1, _) = page1
      val (events2, _) = page2
      expect.all(
        events0.size == 10,
        events1.size == 10,
        events2.size == 5,
        events0.map(_.id) != events1.map(_.id),
        events0.map(_.id) != events2.map(_.id),
        events1.map(_.id) != events2.map(_.id)
      )
    }
  }

  // ── Edge cases ──────────────────────────────────────────────────────────

  test("getEventsPaginated with empty table returns empty results") {
    val repo = new DynamoDbEventRepository(mockClient(Nil), tableName)

    repo.getEventsPaginated(None, 0, 10).map { case (events, total) =>
      expect.all(events.isEmpty, total == 0)
    }
  }

  test("getEventsPaginated with fewer items than pageSize returns all on page 0") {
    val allItems = (1 to 3).map(eventItem).toList
    val repo     = new DynamoDbEventRepository(mockClient(allItems), tableName)

    repo.getEventsPaginated(None, 0, 10).map { case (events, total) =>
      expect.all(
        events.size == 3,
        total == 3
      )
    }
  }

  test("getEventsPaginated page beyond available data returns empty list") {
    val allItems = (1 to 5).map(eventItem).toList
    val repo     = new DynamoDbEventRepository(mockClient(allItems), tableName)

    repo.getEventsPaginated(None, 5, 10).map { case (events, total) =>
      expect.all(events.isEmpty, total == 5)
    }
  }

  // ── City filtering ──────────────────────────────────────────────────────

  test("getEventsPaginated with city filter returns only matching events") {
    val dublinItems = (1 to 3).map { i =>
      val item = eventItem(i)
      item.put("city", AttributeValue.builder().s("Dublin").build())
      item
    }
    val corkItems   = (4 to 6).map { i =>
      val item = eventItem(i)
      item.put("city", AttributeValue.builder().s("Cork").build())
      item
    }
    val allItems    = dublinItems ++ corkItems
    val repo        = new DynamoDbEventRepository(mockClient(allItems.toList), tableName)

    repo.getEventsPaginated(Some("Cork"), 0, 10).map { case (events, total) =>
      expect.all(
        events.size == 3,
        total == 3,
        events.forall(_.city == "Cork")
      )
    }
  }

  test("getEventsPaginated with city filter paginates correctly") {
    val allItems = (1 to 25).map { i =>
      val item = eventItem(i)
      val city = if (i <= 15) "Dublin" else "Cork"
      item.put("city", AttributeValue.builder().s(city).build())
      item
    }.toList
    val repo     = new DynamoDbEventRepository(mockClient(allItems), tableName)

    // Dublin has 15 events — page 0 should have 10, page 1 should have 5
    for {
      dublinPage0 <- repo.getEventsPaginated(Some("Dublin"), 0, 10)
      dublinPage1 <- repo.getEventsPaginated(Some("Dublin"), 1, 10)
    } yield {
      val (events0, total0) = dublinPage0
      val (events1, total1) = dublinPage1
      expect.all(
        events0.size == 10,
        total0 == 15,
        events1.size == 5,
        total1 == 15,
        events0.map(_.id) != events1.map(_.id)
      )
    }
  }

}
