# Tutorial: Building the Ingestion Pipeline

## Goal

Turn the current stub Lambda into a real **Ingestion Lambda** that:
- Fetches events from Ticketmaster (reusing existing code)
- Pushes them into a Kinesis stream
- Can be triggered by **both** API Gateway (HTTP request from frontend) **and** EventBridge cron

We'll build this in **tiny iterative steps**. Each step is a small, verifiable change.

---

## Prerequisites

- `sbt compile` works
- You have a Ticketmaster API key in `http/.env`
- You've read `System_Architecture_Pipeline.md`

---

## Step 1: Understand What We Already Have

Before writing any code, let's trace the existing flow.

### Step 1a — The HTTP server path

Look at `scala/src/main/scala/what/is/on/eire/Main.scala`. Trace what happens:

1. It starts an Ember HTTP server
2. When a request hits `GET /event/pullEvents`, it calls `MainService.pullEvents`
3. `MainService` calls `TicketmasterClient.getEvents("Dublin")`
4. `TicketmasterClient` calls the real Ticketmaster API via smithy4s-generated client
5. It maps the response to `List[IrishEvent]` and returns it

**Try it:** Start the server with `sbt run` and hit the API (you'll need to set up the env var). See that it fetches real events.

### Step 1b — The Lambda path

Look at `infra/src/main/scala/what/is/on/eire/LambdaHandler.scala`. Trace what happens:

1. It implements `RequestStreamHandler` (the Lambda entry point)
2. It creates a **hardcoded** `IrishEvent` — no API call
3. It logs that event
4. It returns a success JSON

**Key insight:** The Lambda currently does NOT call `TicketmasterClient` at all. It just pretends.

### Step 1c — What's missing

The `LambdaHandler` lives in the `infra` module, which **depends on** the `scala` module (see `build.sbt` line 50: `.dependsOn(scalaModule)`).

This means `LambdaHandler` can already access `TicketmasterClient`, `IrishEvent`, etc. — it just doesn't use them yet.

**Done with Step 1.** No code changed.

---

## Step 2: Add Kinesis SDK to the Infra Module

**What we do:** Add one line to `build.sbt` so the `infra` module knows about the AWS Kinesis SDK.

**Why:** The Lambda needs to push events to a Kinesis stream. The AWS SDK gives us `KinesisClient.putRecord()` with proper AWS request signing.

**Edit `build.sbt`:**

Locate the `infra` project definition (around line 38). Currently:

```scala
lazy val infra = (project in file("infra"))
  .settings(
    name := "whats-on-eire-infra",
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-lambda-java-core" % "1.2.3",
      "com.amazonaws" % "aws-lambda-java-events" % "3.16.1"
    ),
    ...
```

Add the Kinesis SDK:

```scala
lazy val infra = (project in file("infra"))
  .settings(
    name := "whats-on-eire-infra",
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-lambda-java-core" % "1.2.3",
      "com.amazonaws" % "aws-lambda-java-events" % "3.16.1",
      "software.amazon.awssdk" % "kinesis" % "2.29.0"     // <-- ADD THIS
    ),
```

**Verify:** Run `sbt infra/compile`. It should succeed. If it doesn't, check the dependency name.

**What changed:** The `infra` module can now import Kinesis classes. No behavior changed yet.

---

## Step 3: Make LambdaHandler Call TicketmasterClient (Instead of Hardcoded Data)

**What we do:** Delete the hardcoded `IrishEvent` in `LambdaHandler` and instead instantiate `TicketmasterClient` to fetch real events.

**Why:** The Lambda should do real work. It already has access to `TicketmasterClient` (via the module dependency). Let's wire it up.

**Replace the contents of `infra/src/main/scala/what/is/on/eire/LambdaHandler.scala`:**

First, add the import for `IORuntime` at the top:

```diff
 package what.is.on.eire

+import cats.effect.unsafe.IORuntime
 import com.amazonaws.services.lambda.runtime.Context
 import com.amazonaws.services.lambda.runtime.RequestStreamHandler
 import java.io.InputStream
 import java.io.OutputStream
 import java.nio.charset.StandardCharsets
```

Next, add an implicit `IORuntime` inside the class and an `apiKey` field:

```diff
 class LambdaHandler extends RequestStreamHandler {

-  // Instantiates perfectly because "infra" module depends on the "scala" module
-  private val processor = new EventProcessor()
+  private given runtime: IORuntime = IORuntime.global
+
+  // Read the API key from environment variable (set in Lambda config)
+  private val apiKey: String =
+    Option(System.getenv("TICKETMASTER_API_KEY"))
+      .getOrElse(throw new RuntimeException("TICKETMASTER_API_KEY env var not set"))

   override def handleRequest(
```

Now replace the entire body of `handleRequest`. Delete the hardcoded event and processor call. Instead, we'll build an HTTP client, create a `TicketmasterApi` client, call it, and log the results:

```scala
  override def handleRequest(
    input: InputStream,
    output: OutputStream,
    context: Context
  ): Unit = {
    val logger = context.getLogger
    logger.log("WhatsOnEire Ingestion Lambda invoked!")

    val rawInput = new String(input.readAllBytes(), StandardCharsets.UTF_8)
    logger.log(s"Incoming event payload (first 200 chars): ${rawInput.take(200)}")

    // ── Build http4s client ──────────────────────────────────────────
    val httpClient: cats.effect.kernel.Resource[cats.effect.IO, org.http4s.client.Client[cats.effect.IO]] =
      org.http4s.ember.client.EmberClientBuilder
        .default[cats.effect.IO]
        .build

    // ── Extract client & build TicketmasterApi in the for-comp ───────
    val program = for {
      client <- httpClient                     // extract Client from Resource
      tmApi  <- smithy4s.http4s.SimpleRestJsonBuilder(TicketmasterApi)
                  .client(client)              // pass extracted Client
                  .uri(org.http4s.Uri.unsafeFromString("https://app.ticketmaster.com"))
                  .resource
    } yield {
      val tmClient = new TicketmasterClient[cats.effect.IO](tmApi, apiKey)
      val events   = tmClient.getEvents("Dublin").unsafeRunSync()
      logger.log(s"Fetched ${events.size} events from Ticketmaster for Dublin")
      events.foreach { e =>
        logger.log(s"  Event: ${e.id} - ${e.title} @ ${e.city}")
      }
    }

    try {
      program.use(_ => cats.effect.IO.unit).unsafeRunSync()
      val result = """{"status": "SUCCESS", "message": "Fetched events from Ticketmaster"}"""
      output.write(result.getBytes(StandardCharsets.UTF_8))
    } catch {
      case e: Exception =>
        logger.log(s"ERROR: ${e.getMessage}")
        e.printStackTrace()
        val error = s"""{"status": "FAILED", "message": "${e.getMessage}"}"""
        output.write(error.getBytes(StandardCharsets.UTF_8))
    }
  }
}
```

The full file should now look like this:

```scala
package what.is.on.eire

import cats.effect.unsafe.IORuntime
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets

class LambdaHandler extends RequestStreamHandler {

  private given runtime: IORuntime = IORuntime.global

  private val apiKey: String =
    Option(System.getenv("TICKETMASTER_API_KEY"))
      .getOrElse(throw new RuntimeException("TICKETMASTER_API_KEY env var not set"))

  override def handleRequest(
    input: InputStream,
    output: OutputStream,
    context: Context
  ): Unit = {
    val logger = context.getLogger
    logger.log("WhatsOnEire Ingestion Lambda invoked!")

    val rawInput = new String(input.readAllBytes(), StandardCharsets.UTF_8)
    logger.log(s"Incoming event payload (first 200 chars): ${rawInput.take(200)}")

    val httpClient: cats.effect.kernel.Resource[cats.effect.IO, org.http4s.client.Client[cats.effect.IO]] =
      org.http4s.ember.client.EmberClientBuilder
        .default[cats.effect.IO]
        .build

    val program = for {
      client <- httpClient                     // extract Client from Resource
      tmApi  <- smithy4s.http4s.SimpleRestJsonBuilder(TicketmasterApi)
                  .client(client)              // pass extracted Client
                  .uri(org.http4s.Uri.unsafeFromString("https://app.ticketmaster.com"))
                  .resource
    } yield {
      val tmClient = new TicketmasterClient[cats.effect.IO](tmApi, apiKey)
      val events   = tmClient.getEvents("Dublin").unsafeRunSync()
      logger.log(s"Fetched ${events.size} events from Ticketmaster for Dublin")
      events.foreach { e =>
        logger.log(s"  Event: ${e.id} - ${e.title} @ ${e.city}")
      }
    }

    try {
      program.use(_ => cats.effect.IO.unit).unsafeRunSync()
      val result = """{"status": "SUCCESS", "message": "Fetched events from Ticketmaster"}"""
      output.write(result.getBytes(StandardCharsets.UTF_8))
    } catch {
      case e: Exception =>
        logger.log(s"ERROR: ${e.getMessage}")
        e.printStackTrace()
        val error = s"""{"status": "FAILED", "message": "${e.getMessage}"}"""
        output.write(error.getBytes(StandardCharsets.UTF_8))
    }
  }
}
```

**Verify:** `sbt infra/compile` succeeds.

**What changed:** The Lambda now calls the real Ticketmaster API and logs real events. It still just logs them — it doesn't push to Kinesis yet. That's the next step.

---

## Step 4: Add a Kinesis Publisher Class

**What we do:** Create a new file `KinesisEventPublisher.scala` in the `infra` module. It will have one job: take an `IrishEvent`, serialize it to JSON, and push it to a Kinesis stream.

**Why:** We're separating concerns. The Lambda handler should orchestrate; it shouldn't contain AWS SDK plumbing. This class wraps the Kinesis SDK details.

**Create `infra/src/main/scala/what/is/on/eire/KinesisEventPublisher.scala`:**

```scala
package what.is.on.eire

import cats.effect.IO

/** Publishes IrishEvent records to an Amazon Kinesis stream. */
class KinesisEventPublisher(streamName: String) {

  /** Pushes a single event to Kinesis.
    *
    * Uses county as the partition key so events from the same county
    * land on the same shard — useful for downstream batch processing.
    */
  def publish(event: IrishEvent): IO[Unit] = IO {
    // For now this is a stub — it just logs.
    // In the next step we'll add the real PutRecord call.
    println(s"[Kinesis] Would publish: ${event.id} to stream: $streamName")
  }
}
```

**Verify:** `sbt infra/compile` succeeds.

**What changed:** We have a placeholder publisher. It doesn't call AWS yet — it just prints. This lets us wire it into the Lambda handler first, see the pipeline flow, then add the real Kinesis call.

---

## Step 5: Wire the Publisher Into the Lambda Handler

**What we do:** Instantiate `KinesisEventPublisher` inside `LambdaHandler` and call `.publish()` after fetching each event.

**Why:** Connect the two pieces. The Lambda now has a complete flow: fetch from Ticketmaster → push to "Kinesis" (still logging for now).

**Edit `infra/src/main/scala/what/is/on/eire/LambdaHandler.scala`:**

In the `program` block, after creating `tmClient`, add the publisher and iterate:

```diff
     val program = for {
       client <- httpClient
       tmApi  <- tmApiResource
     } yield {
       val tmClient = new TicketmasterClient[cats.effect.IO](tmApi, apiKey)
+      val publisher = new KinesisEventPublisher("whats-on-eire-events")
+
       val events   = tmClient.getEvents("Dublin").unsafeRunSync()
       logger.log(s"Fetched ${events.size} events from Ticketmaster for Dublin")
       events.foreach { e =>
         logger.log(s"  Event: ${e.id} - ${e.title} @ ${e.city}")
+        publisher.publish(e).unsafeRunSync()
+        logger.log(s"  -> Published ${e.id} to Kinesis")
       }
     }
```

**Verify:** `sbt infra/compile` succeeds.

**What changed:** The Lambda now has a complete (stub) pipeline: fetch → log → "publish". The `println` from the stub publisher will appear in CloudWatch.

---

## Step 6: Replace the Stub Publisher with a Real Kinesis PutRecord

**What we do:** Add the real `PutRecord` call to `KinesisEventPublisher`. Also add a simple JSON serializer because `IrishEvent` is a case class — we need to turn it into a JSON string for the Kinesis record.

**Why:** Stub is useful for wiring, but we need real data flowing through Kinesis.

**Replace the stub method in `KinesisEventPublisher.scala`:**

```scala
package what.is.on.eire

import cats.effect.IO
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest
import java.nio.charset.StandardCharsets

class KinesisEventPublisher(
  val client: KinesisClient,
  streamName: String
) {

  def publish(event: IrishEvent): IO[Unit] = IO {
    val json = serializeEvent(event)
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
    * In a later tutorial we'll replace this with a proper JSON library
    * (circe or smithy4s generated JSON codecs). For now it works.
    */
  private def serializeEvent(e: IrishEvent): String = {
    val lat = e.coordinates.map(_.latitude).getOrElse(0.0)
    val lng = e.coordinates.map(_.longitude).getOrElse(0.0)
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
```

Now update the companion object with a `resource` factory:

```scala
object KinesisEventPublisher {

  /** Creates a managed Resource that opens and closes the Kinesis client. */
  def resource(streamName: String): cats.effect.Resource[IO, KinesisEventPublisher] =
    cats.effect.Resource.fromAutoCloseable(IO {
      val client = KinesisClient.builder.build
      new KinesisEventPublisher(client, streamName)
    })
}
```

**Update `LambdaHandler.scala`** to use the `resource` factory instead of `new`:

```diff
     val program = for {
       client <- httpClient
       tmApi  <- tmApiResource
+      pub    <- KinesisEventPublisher.resource("whats-on-eire-events")
     } yield {
       val tmClient = new TicketmasterClient[cats.effect.IO](tmApi, apiKey)
-      val publisher = new KinesisEventPublisher("whats-on-eire-events")
-
       val events   = tmClient.getEvents("Dublin").unsafeRunSync()
       logger.log(s"Fetched ${events.size} events from Ticketmaster for Dublin")
       events.foreach { e =>
         logger.log(s"  Event: ${e.id} - ${e.title} @ ${e.city}")
-        publisher.publish(e).unsafeRunSync()
+        pub.publish(e).unsafeRunSync()
         logger.log(s"  -> Published ${e.id} to Kinesis")
       }
     }
```

**Verify:** `sbt infra/compile` succeeds.

**What changed:** The lambda now:
1. Opens an HTTP client → calls Ticketmaster API → fetches real events
2. Opens a Kinesis client → pushes each event as a JSON record to the stream
3. Closes both clients properly when done (via `Resource`)

---

## Step 7: Fetch All Irish Events in One Call

**What we do:** Make `city` optional in the Smithy model, add a `getAllEvents()` method, and switch the Lambda to use it.

**Why:** Looping over a hardcoded city list is fragile (we'd miss cities, and Ticketmaster's own venue data already contains the city). The real Ticketmaster API supports omitting `city` entirely — it then returns **all** events in the country. This is exactly what the cron job wants.

### Step 7a — Make `city` optional in the Smithy model

Open `smithy/src/main/smithy/Ticketmaster.smithy`. Remove `@required` from `city`:

```diff
     @required
     @httpQuery("countryCode")
     countryCode: String,
 
-    @required
     @httpQuery("city")
     city: String,
 
     @required
```

**Why this works:** Smithy4s generates `city: Option[String]`. When `None`, no `&city=` query param is sent → Ticketmaster returns all IE events.

**Regenerate:** Run `sbt smithyModule/compile` to regenerate the Scala code.

### Step 7b — Add `getAllEvents` to `TicketmasterClient`

Edit `scala/src/main/scala/what/is/on/eire/TicketmasterClient.scala`. The generated `getTicketmasterEvents` now takes `(countryCode, apiKey, city: Option[String])`.

Add a convenience method that omits the city parameter:

```scala
  /** Fetch events for a specific Irish city. */
  def getEvents(city: String): F[List[IrishEvent]] =
    api
      .getTicketmasterEvents("IE", apiKey, Some(city))
      .map(toIrishEvents(_, Some(city)))

  /** Fetch ALL events across Ireland (no city filter).
    *
    * Used by the cron-triggered ingestion to get everything
    * without knowing which cities exist.
    */
  def getAllEvents: F[List[IrishEvent]] =
    api
      .getTicketmasterEvents("IE", apiKey, None)
      .map(toIrishEvents(_, None))
```

Also update the private helpers — `requestedCity` is now `Option[String]`:

```diff
   private def toIrishEvents(
     response: TicketmasterResponse,
-    requestedCity: String
+    requestedCity: Option[String]
   ): List[IrishEvent] =
     response._embedded.events.flatMap(toIrishEvent(_, requestedCity))
 
   private def toIrishEvent(
     event: TicketmasterEvent,
-    requestedCity: String
+    requestedCity: Option[String]
   ): Option[IrishEvent] = {
     val venue     = event._embedded.venues.headOption
     val venueCity = venue
       .flatMap(_.city)
       .flatMap(_.name)
-      .getOrElse(requestedCity)
+      .orElse(requestedCity)
+      .getOrElse("Unknown")
```

### Step 7c — Update the Lambda to Parse `irish-location` and Route to Both Triggers

The Lambda needs to handle two trigger sources:
- **Client HTTP:** payload contains `irish-location: "Dublin"` → call `getEvents(city)`
- **Cron:** no location → call `getAllEvents()`

In `LambdaHandler.scala`, add location parsing and route the fetch:

```diff
     val rawInput = new String(input.readAllBytes(), StandardCharsets.UTF_8)
     logger.log(s"Incoming event payload (first 200 chars): ${rawInput.take(200)}")
 
+    // ── Parse the irish-location header from the incoming payload ──
+    val requestedCity: Option[String] = {
+      val headerRegex = """irish-location"\s*:\s*"([^"]+)"""".r
+      rawInput match {
+        case headerRegex(city) => Some(city)
+        case _                 => None  // cron trigger — no location
+      }
+    }
+    logger.log(s"Requested city: ${requestedCity.getOrElse("ALL (cron)")}")
+
     // ── Build http4s client ──────────────────────────────────────────
```

Then replace the hardcoded `getAllEvents` with conditional routing:

```diff
       val tmClient = new TicketmasterClient[cats.effect.IO](tmApi, apiKey)
-      val events   = tmClient.getAllEvents.unsafeRunSync()
-      logger.log(s"Fetched ${events.size} events from Ticketmaster (all Ireland)")
+
+      val fetch = requestedCity match {
+        case Some(city) => tmClient.getEvents(city)
+        case None       => tmClient.getAllEvents
+      }
+      val events = fetch.unsafeRunSync()
+      val label  = requestedCity.getOrElse("all Ireland")
+      logger.log(s"Fetched ${events.size} events from Ticketmaster for $label")
+
       events.foreach { e =>
-        logger.log(s"  Event: ${e.id} - ${e.title} @ ${e.city}, ${e.county}")
+        logger.log(s"  Published: ${e.id} - ${e.title} @ ${e.city}, ${e.county}")
         pub.publish(e).unsafeRunSync()
-        logger.log(s"  -> Published ${e.id} to Kinesis")
       }
+
+      // Return events so the HTTP caller gets them back
+      events
```

Also update the response building — the Lambda now returns the event count:

```diff
     try {
-      program.use(_ => cats.effect.IO.unit).unsafeRunSync()
-      val result = """{"status": "SUCCESS", "message": "Fetched events from Ticketmaster"}"""
+      val events = program.use(events => cats.effect.IO.pure(events)).unsafeRunSync()
+      val result = s"""{"status": "SUCCESS", "count": ${events.size}, "message": "Published ${events.size} events to Kinesis"}"""
       output.write(result.getBytes(StandardCharsets.UTF_8))
     } catch {
       case e: Exception =>
         logger.log(s"ERROR: ${e.getMessage}")
-        e.printStackTrace()
         val error = s"""{"status": "FAILED", "message": "${e.getMessage}"}"""
         output.write(error.getBytes(StandardCharsets.UTF_8))
     }
```

**Verify:** `sbt compile` succeeds.

**What changed:** 
- The Smithy model now allows omitting `city` → the API returns all Irish events
- `TicketmasterClient` has a new `getAllEvents` method
- The Lambda parses `irish-location` from the incoming payload
- Client HTTP → `getEvents(city)`, Cron → `getAllEvents`
- Response now includes the event count

---

## Step 8: Create HttpLambdaRunner — Local API Gateway Stand-In

**What we do:** Create an HTTP server in the `infra` module that receives HTTP requests and invokes `LambdaHandler.handleRequest` programmatically — exactly like API Gateway does in production.

**Why:** To test the full pipeline locally, we need a way to send HTTP requests to the Lambda. `Main.scala` (in the `scala` module) can't depend on `infra`. So we create `HttpLambdaRunner` in `infra` that acts as a local API Gateway.

**Create `infra/src/main/scala/what/is/on/eire/HttpLambdaRunner.scala`:**

```scala
package what.is.on.eire

import cats.effect.{IO, IOApp}
import com.amazonaws.services.lambda.runtime.Context
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits._

/** Local HTTP server that invokes the Ingestion Lambda directly.
  *
  * Stand-in for API Gateway. Serializes the HTTP request into a JSON
  * payload, calls LambdaHandler.handleRequest, and returns the response.
  *
  * Usage:
  *   curl -H "irish-location: Dublin" http://localhost:8080/event/pullEvents
  *   curl http://localhost:8080/cron/trigger
  */
object HttpLambdaRunner extends IOApp.Simple {

  private val handler = new LambdaHandler()

  override def run: IO[Unit] = {

    val routes = HttpRoutes.of[IO] {

      // Client request — extract irish-location header
      case GET -> Root / "event" / "pullEvents" :? LocationHeader(location) =>
        Ok(invokeLambda(location))

      case GET -> Root / "event" / "pullEvents" =>
        BadRequest("Missing irish-location header")

      // Simulate EventBridge cron
      case GET -> Root / "cron" / "trigger" =>
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
      def getAwsRequestId: String = "local-test"
      def getLogGroupName: String = "local"
      def getLogStreamName: String = "local"
      def getFunctionName: String = "whats-on-eire-ingestion"
      def getFunctionVersion: String = "1"
      def getInvokedFunctionArn: String = "arn:aws:lambda:local:test"
      def getIdentity: com.amazonaws.services.lambda.runtime.CognitoIdentity = null
      def getClientContext: com.amazonaws.services.lambda.runtime.ClientContext = null
      def getRemainingTimeInMillis: Int = 30000
      def getLogger: com.amazonaws.services.lambda.runtime.LambdaLogger =
        new com.amazonaws.services.lambda.runtime.LambdaLogger {
          def log(msg: String): Unit = println(s"[Lambda] $msg")
          def log(msg: Array[Byte]): Unit = log(new String(msg, StandardCharsets.UTF_8))
        }
    }

    handler.handleRequest(inputStream, outputStream, dummyContext)
    outputStream.toString(StandardCharsets.UTF_8)
  }

  /** Extracts the irish-location custom header. */
  private object LocationHeader {
    def unapply(req: org.http4s.Request[IO]): Option[String] =
      req.headers.get(org.typelevel.ci.CIString("irish-location")).map(_.head.value)
  }
}
```

**Verify:** `sbt infra/compile` succeeds.

**What changed:** The Lambda can now be invoked via HTTP locally. `HttpLambdaRunner` is the local API Gateway — it wraps the HTTP request into a Lambda event, calls `handleRequest`, and returns the response.

---

## Step 9: Validate and Run Locally

### Complete Flow

```
curl -H "irish-location: Cork" http://localhost:8080/event/pullEvents
         |
         v
  HttpLambdaRunner (port 8080)       <- local API Gateway stand-in
         |
         |-- serializes request -> JSON payload
         |-- calls LambdaHandler.handleRequest(inputStream, outputStream, ctx)
         |
         v
  LambdaHandler
         |-- parses "irish-location" from payload
         |-- if city present:  TicketmasterClient.getEvents("Cork")
         |-- if city absent:   TicketmasterClient.getAllEvents  (cron)
         |-- KinesisEventPublisher.publish(event) for each event
         +-- returns response: { "status": "SUCCESS", "count": 12, ... }

Both trigger sources call the same Lambda with the same code path:
- Client request (city header) -> getEvents -> Kinesis
- EventBridge cron (no header)  -> getAllEvents -> Kinesis
```

### Run It

**Terminal 1 — Start LocalStack and create the Kinesis stream:**

```bash
localstack start -d

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url=http://localhost:4566 --region eu-west-1 \
  kinesis create-stream --stream-name whats-on-eire-events --shard-count 1
```

**Terminal 2 — Start the local API Gateway:**

```bash
export TICKETMASTER_API_KEY=your_key_here
sbt "infra/runMain what.is.on.eire.HttpLambdaRunner"
```

**Terminal 3 — Invoke the Lambda via HTTP:**

```bash
# Client request — fetches events for Dublin
curl -H "irish-location: Dublin" http://localhost:8080/event/pullEvents
# Response: {"status":"SUCCESS","count":87,"message":"Published 87 events to Kinesis"}

# Client request — another city
curl -H "irish-location: Galway" http://localhost:8080/event/pullEvents

# Simulate EventBridge cron — fetches all Ireland
curl http://localhost:8080/cron/trigger
```

**Verify events in Kinesis:**

```bash
ITERATOR=$(AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url=http://localhost:4566 --region eu-west-1 \
  kinesis get-shard-iterator \
    --stream-name whats-on-eire-events \
    --shard-id shardId-000000000000 \
    --shard-iterator-type TRIM_HORIZON \
  --query ShardIterator --output text)

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url=http://localhost:4566 --region eu-west-1 \
  kinesis get-records --shard-iterator $ITERATOR
```

You should see your events as JSON records.

### What's Not Yet Built (Coming Tutorials)

- Processing Lambda (reads from Kinesis, enriches, writes to DynamoDB)
- Geocoding / reverse geocoding
- DynamoDB single-table write
- Analytics path (S3 Firehose -> Databricks)
- Proper JSON parsing (we use regex, will replace with circe)

### Production Deployment

1. Package fat JAR: `sbt infra/assembly`
2. Upload JAR to Lambda
3. Set env var: `TICKETMASTER_API_KEY`
4. Create Kinesis stream in AWS
5. Wire API Gateway + EventBridge triggers

---

## Appendix: File Map After This Tutorial (Steps 1-9)

```
infra/src/main/scala/what/is/on/eire/
|-- LambdaHandler.scala             <- Ingestion Lambda (fetch + publish to Kinesis)
|-- KinesisEventPublisher.scala     <- Wraps PutRecord logic
+-- HttpLambdaRunner.scala          <- NEW: Local API Gateway stand-in

scala/src/main/scala/what/is/on/eire/
|-- Main.scala                      <- Unchanged (dev tool, direct Ticketmaster calls)
|-- MainService.scala               <- Unchanged
|-- TicketmasterClient.scala        <- Updated: +getAllEvents, Option[String] city
+-- EventProcessor.scala            <- Unchanged (used by Processing Lambda later)

smithy/src/main/smithy/
|-- WhatIsOnEire.smithy             <- Unchanged
|-- Ticketmaster.smithy             <- Updated: city is optional
+-- Event.smithy                    <- Unchanged
```
