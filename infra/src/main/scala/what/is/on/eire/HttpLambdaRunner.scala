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
import org.http4s.server.middleware.CORS

/** Local HTTP server that mimics API Gateway.
  *
  * Routes incoming HTTP requests to the appropriate Lambda handler, simulating how API Gateway
  * invokes Lambda functions in production.
  *
  * Usage: curl -H "irish-location: Dublin" http://localhost:8080/event/pullEvents curl
  * http://localhost:8080/getEvents curl -H "irish-location: Dublin" http://localhost:8080/getEvents
  */
object HttpLambdaRunner extends IOApp.Simple {

  private val ingestionHandler = new IngestionLambdaHandler()
  private val getEventsHandler = new GetEventsLambdaHandler()

  override def run: IO[Unit] = {

    val routes = CORS.policy
      .withAllowOriginHost(_ => true)
      .withAllowCredentials(false)
      .apply(HttpRoutes.of[IO] {

        // ── Ingest events from Ticketmaster ────────────────────────────
        case req @ GET -> Root / "event" / "pullEvents" =>
          val city = req.headers
            .get(org.typelevel.ci.CIString("irish-location"))
            .map(_.head.value)
          Ok(invokeLambda(city))

        // Simulate EventBridge cron
        case GET -> Root / "cron" / "trigger"           =>
          Ok(invokeLambda(None))

        // ── Read events from DynamoDB ──────────────────────────────────
        case req @ GET -> Root / "getEvents"            =>
          val city      = req.headers
            .get(org.typelevel.ci.CIString("irish-location"))
            .map(_.head.value)
          val rawParams = req.uri.query.params
          println(s"[HttpLambdaRunner] query params: $rawParams")
          val page      = rawParams.get("page").flatMap(_.toIntOption).getOrElse(0)
          val pageSize  = rawParams.get("pageSize").flatMap(_.toIntOption).getOrElse(10)
          println(s"[HttpLambdaRunner] page=$page pageSize=$pageSize city=$city")
          Ok(invokeReadLambda(city, page, pageSize))
      })

    val port = com.comcast.ip4s.Port
      .fromInt(
        EnvLoader.get("APP_PORT").flatMap(p => scala.util.Try(p.toInt).toOption).getOrElse(8080)
      )
      .getOrElse(com.comcast.ip4s.Port.fromInt(8080).get)

    EmberServerBuilder
      .default[IO]
      .withPort(port)
      .withHttpApp(routes.orNotFound)
      .build
      .useForever
  }

  /** Build a Lambda input payload and invoke the Ingestion Lambda.
    *
    * When `city` is `Some`, only that city's events are fetched. When `city` is `None`, ALL events
    * across Ireland are fetched.
    */
  private def invokeLambda(city: Option[String]): String = {
    val headerField = city match {
      case Some(c) => s""""irish-location": "$c""""
      case None    => ""
    }
    val inputJson   =
      s"""{
         |  "headers": { $headerField },
         |  "body": "{}"
         |}""".stripMargin

    invokeHandler(ingestionHandler, inputJson)
  }

  /** Build a Lambda input payload and invoke the GetEvents Lambda with pagination. */
  private def invokeReadLambda(
    city: Option[String],
    page: Int = 0,
    pageSize: Int = 10
  ): String = {
    val headerField = city match {
      case Some(c) => s""""irish-location": "$c""""
      case None    => ""
    }
    val inputJson   =
      s"""{
         |  "headers": { $headerField },
         |  "page": $page,
         |  "pageSize": $pageSize
         |}""".stripMargin

    invokeHandler(getEventsHandler, inputJson)
  }

  /** Serialize the HTTP request into a JSON payload, call the Lambda handler, and return the
    * response string.
    */
  private def invokeHandler(
    handler: com.amazonaws.services.lambda.runtime.RequestStreamHandler,
    inputJson: String
  ): String = {
    val inputStream  = new ByteArrayInputStream(inputJson.getBytes(StandardCharsets.UTF_8))
    val outputStream = new ByteArrayOutputStream()

    val dummyContext = new Context {
      def getAwsRequestId: String                                               = "local-test"
      def getLogGroupName: String                                               = "local"
      def getLogStreamName: String                                              = "local"
      def getFunctionName: String                                               = "whats-on-eire-local"
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
