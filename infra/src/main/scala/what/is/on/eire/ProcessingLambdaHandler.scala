package what.is.on.eire

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

/** Kinesis-triggered Lambda that processes raw event records and persists them to DynamoDB.
  *
  * Uses `RequestHandler[KinesisEvent, String]` so AWS's Java runtime deserialises the Kinesis event
  * automatically — no manual serde needed.
  */
class ProcessingLambdaHandler extends RequestHandler[KinesisEvent, String] {

  private given runtime: IORuntime = IORuntime.global

  private val tableName: String =
    Option(System.getenv("EVENTS_TABLE_NAME"))
      .getOrElse(throw new RuntimeException("EVENTS_TABLE_NAME env var not set"))

  override def handleRequest(event: KinesisEvent, context: Context): String = {
    val logger = context.getLogger
    logger.log("WhatsOnEire Processing Lambda invoked!")

    val records: List[KinesisEvent.KinesisEventRecord] =
      event.getRecords.asScala.toList

    logger.log(s"Received ${records.size} records from Kinesis stream")

    val jsonRecords: List[String] = records.map { record =>
      val data  = record.getKinesis.getData
      val bytes = new Array[Byte](data.remaining())
      data.get(bytes)
      new String(bytes, StandardCharsets.UTF_8)
    }

    jsonRecords.foreach { json =>
      logger.log(s"  Record: ${json.take(120)}")
    }

    val localstackPort =
      Option(System.getenv("LOCALSTACK_PORT")).flatMap(p => scala.util.Try(p.toInt).toOption)

    val program = DynamoDbEventRepository.resource(tableName, localstackPort).use { repo =>
      val service = new EventStoreService[IO](repo.saveEvent)
      service.processBatch(jsonRecords).flatMap { summary =>
        IO(logger.log(s"Processing complete: $summary"))
      }
    }

    try {
      val summary = program.unsafeRunSync()
      s"""{"status": "SUCCESS", "message": "$summary"}"""
    } catch {
      case e: Exception =>
        logger.log(s"ERROR: ${e.getMessage}\n${e.getStackTrace.map(_.toString).mkString("\n")}")
        s"""{"status": "FAILED", "message": "${e.getMessage}"}"""
    }
  }
}
