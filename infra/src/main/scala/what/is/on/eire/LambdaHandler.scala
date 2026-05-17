package what.is.on.eire

import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets

class LambdaHandler extends RequestStreamHandler {
  
  // Instantiates perfectly because "infra" module depends on the "scala" module
  private val processor = new EventProcessor()

  override def handleRequest(input: InputStream, output: OutputStream, context: Context): Unit = {
    val logger = context.getLogger
    logger.log("WhatsOnEire Engine invoked successfully via LocalStack baseline!")

    val rawInput = new String(input.readAllBytes(), StandardCharsets.UTF_8)
    logger.log(s"Received Cloud Event Payload: $rawInput")

    val sampleEvent = IrishEvent(
      id = "evt-10294-galway",
      title = "Galway Arts Festival Matchup",
      url = "https://whatisoneire.com/events/galway-arts",
      startDate = "2026-07-20",
      city = "Galway",
      county = IrishCounty.GALWAY,
      source = "internal-scraper",
      startTime = Some("19:30"),
      coordinates = Some(GeoCoordinates(latitude = 53.2707, longitude = -9.0568))
    )

    // Hand the event off directly to your domain brain
    val domainResult = processor.process(sampleEvent)
    logger.log(domainResult)

    val resultResponse = s"""{"status": "SUCCESS", "message": "$domainResult"}"""
    output.write(resultResponse.getBytes(StandardCharsets.UTF_8))
  }
}