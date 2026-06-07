package what.is.on.eire

import cats.effect.IO
import java.util.HashMap
import scala.jdk.CollectionConverters._
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

/** Persists [[IrishEvent]] records into the DynamoDB `irish-events` table.
  *
  * Uses the single-table design:
  *   - Partition Key (PK): `county_id` — e.g. "IE-D" for Dublin
  *   - Sort Key (SK): `event_date_id` — e.g. "2026-08-15#Z7r9jZ1AdOkT9"
  *
  * This class is intentionally NOT unit-tested — it is a thin wrapper over the AWS SDK. Test it via
  * LocalStack integration tests or rely on the [[EventStoreService]] unit tests which cover the
  * orchestration logic.
  */
class DynamoDbEventRepository(
  val client: DynamoDbClient,
  tableName: String
) {

  /** Persist a single [[IrishEvent]] into DynamoDB.
    *
    * @param event
    *   the event to save
    * @return
    *   a descriptive string with the event ID and title
    */
  def saveEvent(event: IrishEvent): IO[String] = IO {
    val item = new HashMap[String, AttributeValue]()

    // Partition Key: county_id (e.g. "IE-D")
    item.put("county_id", AttributeValue.builder().s(event.county.toString).build())

    // Sort Key: event_date_id (e.g. "2026-08-15#Z7r9jZ1AdOkT9")
    item.put("event_date_id", AttributeValue.builder().s(s"${event.startDate}#${event.id}").build())

    // Data attributes
    item.put("id", AttributeValue.builder().s(event.id).build())
    item.put("title", AttributeValue.builder().s(event.title).build())
    item.put("url", AttributeValue.builder().s(event.url).build())
    item.put("startDate", AttributeValue.builder().s(event.startDate).build())
    item.put("city", AttributeValue.builder().s(event.city).build())
    item.put("county", AttributeValue.builder().s(event.county.toString).build())
    item.put("source", AttributeValue.builder().s(event.source).build())

    // Optional fields
    event.startTime.foreach { t =>
      item.put("startTime", AttributeValue.builder().s(t).build())
    }
    event.coordinates.foreach { c =>
      item.put("latitude", AttributeValue.builder().n(c.latitude.toString).build())
      item.put("longitude", AttributeValue.builder().n(c.longitude.toString).build())
    }

    val request = PutItemRequest.builder
      .tableName(tableName)
      .item(item)
      .build()

    client.putItem(request)
    s"${event.id} - ${event.title}"
  }

  /** Query events from DynamoDB, optionally filtered by city.
    *
    * @param city
    *   if provided, only events matching this city (case-insensitive) are returned
    * @return
    *   a list of [[IrishEvent]] objects
    */
  def getEvents(city: Option[String]): IO[List[IrishEvent]] = IO {
    val scanRequest = ScanRequest.builder
      .tableName(tableName)
      .build()

    val result   = client.scan(scanRequest)
    val allItems = result.items().asScala.toList.map(itemToEvent)

    city match {
      case Some(c) => allItems.filter(_.city.equalsIgnoreCase(c))
      case None    => allItems
    }
  }

  /** Convert a DynamoDB item (Map[String, AttributeValue]) to an [[IrishEvent]]. */
  private def itemToEvent(item: java.util.Map[String, AttributeValue]): IrishEvent = {
    val attrs = item.asScala

    def s(key: String): String            = attrs.get(key).map(_.s()).getOrElse("")
    def optS(key: String): Option[String] = attrs.get(key).map(_.s()).filter(_.nonEmpty)
    def optD(key: String): Option[Double] =
      attrs.get(key).flatMap(a => scala.util.Try(a.n().toDouble).toOption)

    val county = IrishCounty.values
      .find(_.name == s("county"))
      .getOrElse(IrishCounty.UNKNOWN)

    val coordinates = for {
      lat <- optD("latitude")
      lng <- optD("longitude")
    } yield GeoCoordinates(lat, lng)

    IrishEvent(
      id = s("id"),
      title = s("title"),
      url = s("url"),
      startDate = s("startDate"),
      startTime = optS("startTime"),
      city = s("city"),
      county = county,
      coordinates = coordinates,
      source = s("source")
    )
  }

}

object DynamoDbEventRepository {

  /** Creates a managed Resource that opens and closes the DynamoDB client.
    *
    * @param tableName
    *   the DynamoDB table name
    * @param localstackPort
    *   if set, connects to LocalStack instead of real AWS
    */
  def resource(
    tableName: String,
    localstackPort: Option[Int] = None
  ): cats.effect.Resource[IO, DynamoDbEventRepository] =
    cats.effect.Resource.make(
      acquire = IO {
        val builder = DynamoDbClient.builder
        localstackPort.foreach { port =>
          builder
            .endpointOverride(java.net.URI.create(s"http://localhost:$port"))
            .region(software.amazon.awssdk.regions.Region.EU_WEST_1)
            .credentialsProvider(
              software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("test", "test")
              )
            )
        }
        val client  = builder.build
        new DynamoDbEventRepository(client, tableName)
      }
    )(release = repo => IO(repo.client.close()))
}
