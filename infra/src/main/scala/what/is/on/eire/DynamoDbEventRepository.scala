package what.is.on.eire

import cats.effect.IO
import java.util.HashMap
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest

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
