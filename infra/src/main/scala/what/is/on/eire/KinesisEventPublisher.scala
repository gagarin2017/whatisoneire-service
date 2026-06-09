package what.is.on.eire

import cats.effect.IO
import java.net.URI
import java.nio.charset.StandardCharsets
import smithy4s.Schema
import smithy4s.json.Json
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

  /** Serialize an [[IrishEvent]] to JSON using smithy4s generated codecs. */
  private def serializeEvent(e: IrishEvent): String = {
    val encoder = Json.payloadCodecs.encoders.fromSchema(Schema[IrishEvent])
    val blob    = encoder.encode(e)
    new String(blob.toArray, StandardCharsets.UTF_8)
  }
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
