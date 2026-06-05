package what.is.on.eire

import cats.effect.IO
import java.net.URI
import java.nio.charset.StandardCharsets
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest

class KinesisEventPublisher(
  val client: KinesisClient,
  streamName: String
) {

  def publish(event: IrishEvent): IO[Unit] = IO {
    val json    = serializeEvent(event)
    val request = PutRecordRequest.builder
      .streamName(streamName)
      .partitionKey(event.county.toString)
      .data(SdkBytes.fromString(json, StandardCharsets.UTF_8))
      .build
    client.putRecord(request)
    ()
  }

  /** Minimal JSON serialization for IrishEvent.
    *
    * In a later tutorial we'll replace this with a proper JSON library (circe or smithy4s generated
    * JSON codecs). For now it works.
    */
  private def serializeEvent(e: IrishEvent): String = {
    val lat       = e.coordinates.map(_.latitude).getOrElse(0.0)
    val lng       = e.coordinates.map(_.longitude).getOrElse(0.0)
    val timeField = e.startTime.map(t => s""""startTime": "$t",""").getOrElse("")
    s"""{
       |  "id": "${e.id}",
       |  "title": "${escape(e.title)}",
       |  "url": "${escape(e.url)}",
       |  "startDate": "${e.startDate}",
       |  ${timeField}
       |  "city": "${escape(e.city)}",
       |  "county": "${e.county}",
       |  "coordinates": { "latitude": $lat, "longitude": $lng },
       |  "source": "${e.source}"
       |}""".stripMargin
  }

  private def escape(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"")
}

object KinesisEventPublisher {

  /** Creates a managed Resource that opens and closes the Kinesis client.
    *
    * @param streamName
    *   The Kinesis stream name
    * @param localstackPort
    *   If set, connects to LocalStack at this port (e.g. 4566) instead of real AWS. Used for local
    *   testing.
    */
  def resource(
    streamName: String,
    localstackPort: Option[Int] = None
  ): cats.effect.Resource[IO, KinesisEventPublisher] =
    cats.effect.Resource.make(
      acquire = IO {
        val builder = KinesisClient.builder
        localstackPort.foreach { port =>
          builder
            .endpointOverride(URI.create(s"http://localhost:$port"))
            .region(Region.EU_WEST_1)
            .credentialsProvider(
              StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
            )
        }
        val client  = builder.build
        new KinesisEventPublisher(client, streamName)
      }
    )(release = pub => IO(pub.client.close()))
}
