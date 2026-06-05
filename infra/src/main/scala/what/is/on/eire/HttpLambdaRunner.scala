package what.is.on.eire

import cats.effect.IO
import cats.effect.IOApp
import com.amazonaws.services.lambda.runtime.Context
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits._

/** Local HTTP server that invokes the Ingestion Lambda directly.
  *
  * Stand-in for API Gateway. Serializes the HTTP request into a JSON payload, calls
  * LambdaHandler.handleRequest, and returns the response.
  *
  * Usage: curl -H "irish-location: Dublin" http://localhost:8080/event/pullEvents curl
  * http://localhost:8080/cron/trigger
  */
object HttpLambdaRunner extends IOApp.Simple {

  private val handler = new IngestionLambdaHandler()

  override def run: IO[Unit] = {

    val routes = HttpRoutes.of[IO] {

      // Client request — extract irish-location header
      case req @ GET -> Root / "event" / "pullEvents" =>
        val location = req.headers
          .get(org.typelevel.ci.CIString("irish-location"))
          .map(_.head.value)
        location match {
          case Some(city) => Ok(invokeLambda(city))
          case None       => BadRequest("Missing irish-location header")
        }

      // Simulate EventBridge cron
      case GET -> Root / "cron" / "trigger"           =>
        Ok(invokeLambda("cron"))
    }

    EmberServerBuilder
      .default[IO]
      .withHttpApp(routes.orNotFound)
      .build
      .useForever
  }

  /** Serializes an HTTP request into a Lambda invocation. */
  private def invokeLambda(location: String): String = {
    val inputJson =
      s"""{
         |  "headers": { "irish-location": "$location" },
         |  "body": "{}"
         |}""".stripMargin

    val inputStream  = new ByteArrayInputStream(inputJson.getBytes(StandardCharsets.UTF_8))
    val outputStream = new ByteArrayOutputStream()

    // Dummy Lambda context (LambdaRuntime provides this in production)
    val dummyContext = new Context {
      def getAwsRequestId: String                                               = "local-test"
      def getLogGroupName: String                                               = "local"
      def getLogStreamName: String                                              = "local"
      def getFunctionName: String                                               = "whats-on-eire-ingestion"
      def getFunctionVersion: String                                            = "1"
      def getInvokedFunctionArn: String                                         = "arn:aws:lambda:local:test"
      def getIdentity: com.amazonaws.services.lambda.runtime.CognitoIdentity    = null
      def getClientContext: com.amazonaws.services.lambda.runtime.ClientContext = null
      def getRemainingTimeInMillis: Int                                         = 30000
      def getMemoryLimitInMB: Int                                               = 512
      def getLogger: com.amazonaws.services.lambda.runtime.LambdaLogger         =
        new com.amazonaws.services.lambda.runtime.LambdaLogger {
          def log(msg: String): Unit      = println(s"[Lambda] $msg")
          def log(msg: Array[Byte]): Unit = log(new String(msg, StandardCharsets.UTF_8))
        }
    }

    handler.handleRequest(inputStream, outputStream, dummyContext)
    outputStream.toString(StandardCharsets.UTF_8)
  }

}
