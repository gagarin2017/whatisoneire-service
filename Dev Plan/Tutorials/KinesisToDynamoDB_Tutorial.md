# Tutorial: Build a Kinesis-to-DynamoDB Processing Lambda with Weaver Tests

## Goal

Add a **Processing Lambda** that reads events from the Kinesis stream (`events-raw-stream`) and persists them into the DynamoDB table (`irish-events`). This completes the second half of the ingestion pipeline.

We create the following new files:

```
scala/src/main/scala/what/is/on/eire/
├── KinesisRecordParser.scala    ← NEW: deserialises a Kinesis JSON record into IrishEvent
└── EventStoreService.scala      ← NEW: parses Kinesis records + saves via injected function

scala/src/test/scala/what/is/on/eire/
├── KinesisRecordParserSpec.scala  ← 8 Weaver test cases
└── EventStoreServiceSpec.scala    ← 3 Weaver test cases

infra/src/main/scala/what/is/on/eire/
├── DynamoDbEventRepository.scala  ← NEW: DynamoDB putItem implementation
└── ProcessingLambdaHandler.scala  ← NEW: Kinesis-triggered Lambda handler
```

And update one existing file:

```
build.sbt  ← UPDATED: add DynamoDB SDK dependency
```

---

## Prerequisites

- `sbt compile` works
- You have completed the [IngestionPipeline_Tutorial.md](./IngestionPipeline_Tutorial.md) (the Kinesis stream exists and events are being published)
- The DynamoDB table `irish-events` exists (provisioned by `infra-cdk`)
- You understand the existing `IrishEvent` model and its JSON shape as serialised by `KinesisEventPublisher`

---

## Step 0: Understand the Architecture

Before writing code, let's see where we are and where we're going.

### Current flow (pipeline is half-built)

```
EventBridge Cron ──→ IngestionLambda ──→ Ticketmaster API ──→ Kinesis Stream
                                                                    │
                                                               (events sit here)
                                                                    │
                                                              ❌ NOTHING reads them yet
```

Events are successfully fetched from Ticketmaster and published to Kinesis, but **no consumer reads from Kinesis**. The DynamoDB table `irish-events` exists but stays empty.

### Target flow

```
EventBridge Cron ──→ IngestionLambda ──→ Ticketmaster API ──→ Kinesis Stream
                                                                    │
                                                            Kinesis Event Source Mapping
                                                                    │
                                                                    ▼
                                                         ProcessingLambdaHandler
                                                                    │
                                                         DynamoDbEventRepository
                                                                    │
                                                                    ▼
                                                         DynamoDB (irish-events)
```

The **Processing Lambda** is triggered by a Kinesis Event Source Mapping (set up via CDK). It does:

1. Receive a batch of Kinesis records (each contains a JSON-serialised `IrishEvent`)
2. Parse each record with `KinesisRecordParser`
3. Save each parsed event with `DynamoDbEventRepository`
4. Log results

### Where each piece lives

| Component | Module | Role |
|---|---|---|
| `KinesisRecordParser` | `scala` | Pure JSON → `IrishEvent` parsing. No AWS dependencies. Weaver-testable. |
| `EventStoreService` | `scala` | Orchestrates parsing + saving via injected function. No AWS deps. Testable. |
| `DynamoDbEventRepository` | `infra` | Real DynamoDB `putItem` call. Not unit-testable (test via LocalStack). |
| `ProcessingLambdaHandler` | `infra` | Kinesis-triggered Lambda. Thin infra wrapper. |

The pattern is the same as `IngestionService`/`IngestionLambdaHandler`: the **service** lives in `scala`, takes a pure function for its dependency, and is tested with `IO.pure`. The **Lambda handler** and **repository** live in `infra` and do the real I/O.

---

## Step 1: Add DynamoDB SDK to the Infra Module

The infra module currently depends on `kinesis` SDK but not `dynamodb`. We need the DynamoDB SDK to create `DynamoDbEventRepository`.

### Step 1a — Navigate

Open the project root.

### Step 1b — Open `build.sbt`

### Step 1c — Find the `infra` project definition

Locate lines around 37-49:

```scala
lazy val infra = (project in file("infra"))
  .settings(
    name                             := "whats-on-eire-infra",
    libraryDependencies ++= Seq(
      "com.amazonaws"          % "aws-lambda-java-core"   % "1.2.3",
      "com.amazonaws"          % "aws-lambda-java-events" % "3.16.1",
      "software.amazon.awssdk" % "kinesis"                % "2.29.0"
    ),
    ...
```

### Step 1d — Add the DynamoDB dependencies

Add the DynamoDB SDK and its HTTP client, and fix the assembly merge strategy to preserve SPI service files (the AWS SDK uses `META-INF/services` to discover HTTP implementations):

```scala
    libraryDependencies ++= Seq(
      "com.amazonaws"          % "aws-lambda-java-core"   % "1.2.3",
      "com.amazonaws"          % "aws-lambda-java-events" % "3.16.1",
      "software.amazon.awssdk" % "kinesis"                % "2.29.0",
      "software.amazon.awssdk" % "dynamodb"               % "2.29.0",
      "software.amazon.awssdk" % "url-connection-client"  % "2.29.0"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*)        => MergeStrategy.discard
      case _                                    => MergeStrategy.first
    }
```

### Step 1e — Verify it compiles

```bash
sbt compile
```

Expected output: `[success]` with no errors.

---

## Step 2: Understand the Kinesis Record Shape

When a Kinesis stream triggers a Lambda via an Event Source Mapping, the input is not a single record — it's a **batch** wrapped in a specific AWS format.

The `aws-lambda-java-events` library provides the `KinesisEvent` class which models this. A `KinesisEvent` contains a list of `KinesisEventRecord` objects. Each record has:

- `record.getkinesis.getData()` — a `ByteBuffer` containing the **base64-decoded** JSON payload
- `record.getkinesis.getPartitionKey()` — the partition key
- `record.getkinesis.getSequenceNumber()` — the sequence number

The payload inside each record is exactly the JSON that `KinesisEventPublisher.serializeEvent(...)` wrote.

**Example Kinesis record data (after base64 decode):**

```json
{
  "id": "Z7r9jZ1AdOkT9",
  "title": "Live Traditional Session",
  "url": "https://www.ticketmaster.ie/...",
  "startDate": "2026-08-15",
  "startTime": "20:00:00",
  "city": "Dublin",
  "county": "DUBLIN",
  "coordinates": { "latitude": 53.3498, "longitude": -6.2603 },
  "source": "Ticketmaster"
}
```

Note: `county` is a **string** (the enum name, e.g. `"DUBLIN"`) in the JSON. We'll need to parse it back into `IrishCounty`.

---

## Step 3: Create `KinesisRecordParser.scala`

This pure utility parses a JSON string (from a single Kinesis record) into an `IrishEvent` case class.

### Step 3a — Navigate

```
scala/src/main/scala/what/is/on/eire/
```

You should see:

```
CityHeaderParser.scala
EventProcessor.scala
IngestionService.scala
Main.scala
MainService.scala
TicketmasterClient.scala
```

### Step 3b — Create the file

Create a new file named `KinesisRecordParser.scala` in this directory.

### Step 3c — Add the package declaration

```scala
package what.is.on.eire
```

Press Enter twice.

### Step 3d — Add the Scaladoc comment

```scala
/** Pure utility that deserialises a JSON string (from a single Kinesis record)
  * into an [[IrishEvent]] case class.
  *
  * Expects the JSON shape produced by [[KinesisEventPublisher.serializeEvent]].
  * Uses minimal string parsing — no JSON library dependency.
  *
  * Only `id`, `title`, `city`, `county`, and `source` are hard-required.
  * `url` and `startDate` default to `""` when absent. Unrecognised county
  * strings fall back to [[IrishCounty.UNKNOWN]]. This ensures events are saved
  * even if the source data is incomplete.
  *
  * Thread-safe: all parsing is stateless.
  */
```

### Step 3e — Add the object declaration

```scala
object KinesisRecordParser {
```

### Step 3f — Add the `parse` method

Indent 2 spaces inside the object, type:

```scala
def parse(json: String): Option[IrishEvent] = {
    // ── Helper: extract a raw string value for a given JSON key ──
    def extractString(key: String): Option[String] = {
      val regex = s""""$key"\\s*:\\s*"([^"]*)"""".r
      regex.findFirstMatchIn(json).map(_.group(1))
    }

    // ── Helper: extract an optional string (may be absent or null) ──
    def extractOptionalString(key: String): Option[String] =
      extractString(key).filter(_.nonEmpty)

    // ── Helper: extract a nested object's fields ──
    def extractNestedString(outerKey: String, innerKey: String): Option[String] = {
      val regex = s""""$outerKey"\\s*:\\s*\\{[^}]*"$innerKey"\\s*:\\s*([^,}]+)""".r
      regex.findFirstMatchIn(json).map(_.group(1).trim.replace("\"", ""))
    }

    for {
      id        <- extractString("id")
      title     <- extractString("title")
      city      <- extractString("city")
      countyStr <- extractString("county")
      source    <- extractString("source")
    } yield IrishEvent(
      id = id,
      title = title,
      url = extractString("url").getOrElse(""),
      startDate = extractString("startDate").getOrElse(""),
      startTime = extractOptionalString("startTime"),
      city = city,
      county = parseCounty(countyStr),
      coordinates = parseCoordinates(json),
      source = source
    )
  }
```

### Step 3g — Add the private helper methods

After the `parse` method, add:

```scala
  /** Parse a JSON county string back into an [[IrishCounty]] enum value.
    * Unrecognised county strings fall back to [[IrishCounty.UNKNOWN]] so the
    * event is still saved to the database.
    */
  private def parseCounty(s: String): IrishCounty =
    s.trim.toUpperCase match {
      case "DUBLIN"  => IrishCounty.DUBLIN
      case "GALWAY"  => IrishCounty.GALWAY
      case "CORK"    => IrishCounty.CORK
      case "MEATH"   => IrishCounty.MEATH
      case "UNKNOWN" => IrishCounty.UNKNOWN
      case _         => IrishCounty.UNKNOWN
    }

  /** Parse an optional coordinates object from the JSON. */
  private def parseCoordinates(json: String): Option[GeoCoordinates] = {
    val latRegex = """"latitude"\s*:\s*([0-9.-]+)""".r
    val lngRegex = """"longitude"\s*:\s*([0-9.-]+)""".r
    for {
      latStr <- latRegex.findFirstMatchIn(json).map(_.group(1))
      lngStr <- lngRegex.findFirstMatchIn(json).map(_.group(1))
      lat    <- latStr.toDoubleOption
      lng    <- lngStr.toDoubleOption
    } yield GeoCoordinates(lat, lng)
  }
```

### Step 3h — Close the object

Type `}` on a new line.

### Step 3i — Verify it compiles

```bash
sbt compile
```

Expected output: `[success]` with no errors.

### Step 3j — Walk through what you just wrote

| Piece | What it does |
|---|---|
| `object KinesisRecordParser` | Singleton — stateless, thread-safe |
| `def parse(json): Option[IrishEvent]` | Main entry point. Returns `None` if any hard-required field is missing |
| `extractString(key)` | Finds `"key": "value"` anywhere in the JSON string using substring regex |
| `extractOptionalString(key)` | Same but returns `None` for empty strings (used for optional `startTime`) |
| `parseCounty(s)` | Converts the JSON string `"DUBLIN"` → `IrishCounty.DUBLIN`. Unrecognised → `UNKNOWN` |
| `parseCoordinates(json)` | Extracts latitude/longitude from optional coordinates object |
| `for { ... } yield` | Only `id`, `title`, `city`, `county`, `source` are in the `for` — they must be present. `url` and `startDate` use `.getOrElse("")` outside the comprehension |

---

## Step 4: Create `KinesisRecordParserSpec.scala`

### Step 4a — Navigate

```
scala/src/test/scala/what/is/on/eire/
```

You should see:

```
CityHeaderParserSpec.scala
EventProcessorSpec.scala
IngestionServiceSpec.scala
resource/
```

### Step 4b — Create the test file

Create a new file named `KinesisRecordParserSpec.scala` in this directory.

### Step 4c — Package and imports

```scala
package what.is.on.eire

import weaver.SimpleIOSuite
import cats.effect.IO
```

Press Enter twice.

### Step 4d — Test class declaration

```scala
object KinesisRecordParserSpec extends SimpleIOSuite {
```

### Step 4e — Test 1: happy path — full event with coordinates

Indent 2 spaces, type:

```scala
test("parse returns an IrishEvent for valid JSON with all fields") {
    val json =
      """{
        |  "id": "Z7r9jZ1AdOkT9",
        |  "title": "Live Traditional Session",
        |  "url": "https://ticketmaster.ie/test",
        |  "startDate": "2026-08-15",
        |  "startTime": "20:00:00",
        |  "city": "Dublin",
        |  "county": "DUBLIN",
        |  "coordinates": { "latitude": 53.3498, "longitude": -6.2603 },
        |  "source": "Ticketmaster"
        |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect.all(
        result.isDefined,
        result.get.id == "Z7r9jZ1AdOkT9",
        result.get.title == "Live Traditional Session",
        result.get.city == "Dublin",
        result.get.county == IrishCounty.DUBLIN,
        result.get.startTime == Some("20:00:00"),
        result.get.coordinates.isDefined,
        result.get.source == "Ticketmaster"
      )
    }
  }
```

Press Enter twice.

### Step 4f — Test 2: no startTime → parses successfully with None

```scala
test("parse succeeds when startTime is absent (optional field)") {
    val json =
      """{
        |  "id": "EVT001",
        |  "title": "Galway Arts Festival",
        |  "url": "https://ticketmaster.ie/galway",
        |  "startDate": "2026-07-20",
        |  "city": "Galway",
        |  "county": "GALWAY",
        |  "source": "Ticketmaster"
        |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect.all(
        result.isDefined,
        result.get.startTime == None,
        result.get.county == IrishCounty.GALWAY
      )
    }
  }
```

Press Enter twice.

### Step 4g — Test 3: no coordinates → parses successfully with None

```scala
test("parse succeeds when coordinates are absent (optional field)") {
    val json =
      """{
        |  "id": "EVT002",
        |  "title": "Cork Jazz Weekend",
        |  "url": "https://ticketmaster.ie/cork",
        |  "startDate": "2026-10-25",
        |  "city": "Cork",
        |  "county": "CORK",
        |  "source": "Ticketmaster"
        |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect.all(
        result.isDefined,
        result.get.coordinates == None,
        result.get.county == IrishCounty.CORK
      )
    }
  }
```

Press Enter twice.

### Step 4h — Test 4: missing hard-required field (id) → returns None

```scala
test("parse returns None when a hard-required field (id) is missing") {
    val json =
      """{
        |  "title": "No ID Event",
        |  "city": "Dublin",
        |  "county": "DUBLIN",
        |  "source": "Ticketmaster"
        |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect(result == None)
    }
  }
```

Press Enter twice.

### Step 4i — Test 5: missing url → succeeds with empty string

```scala
test("parse succeeds when url is missing (optional field — defaults to empty string)") {
    val json =
      """{
        |  "id": "EVT100",
        |  "title": "No URL Event",
        |  "startDate": "2026-06-01",
        |  "city": "Dublin",
        |  "county": "DUBLIN",
        |  "source": "Ticketmaster"
        |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect.all(
        result.isDefined,
        result.get.url == ""
      )
    }
  }
```

Press Enter twice.

### Step 4j — Test 6: missing startDate → succeeds with empty string

```scala
test("parse succeeds when startDate is missing (optional field — defaults to empty string)") {
    val json =
      """{
        |  "id": "EVT101",
        |  "title": "No Date Event",
        |  "url": "https://ticketmaster.ie/no-date",
        |  "city": "Galway",
        |  "county": "GALWAY",
        |  "source": "Ticketmaster"
        |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect.all(
        result.isDefined,
        result.get.startDate == ""
      )
    }
  }
```

Press Enter twice.

### Step 4k — Test 7: unknown county → succeeds with UNKNOWN

```scala
test("parse succeeds with UNKNOWN county when county is not a recognised IrishCounty") {
    val json =
      """{
        |  "id": "EVT003",
        |  "title": "Mystery Event",
        |  "url": "https://ticketmaster.ie/mystery",
        |  "startDate": "2026-06-01",
        |  "city": "Nowhere",
        |  "county": "NARNIA",
        |  "source": "Ticketmaster"
        |}""".stripMargin

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect.all(
        result.isDefined,
        result.get.county == IrishCounty.UNKNOWN
      )
    }
  }
```

Press Enter twice.

### Step 4l — Test 8: empty string payload → returns None

```scala
test("parse returns None for an empty JSON object") {
    val json = "{}"

    IO(KinesisRecordParser.parse(json)).map { result =>
      expect(result == None)
    }
  }
```

### Step 4m — Close the object

Type `}` on a new line.

### Step 4n — Run the tests

```bash
sbt "scalaModule/testOnly what.is.on.eire.KinesisRecordParserSpec"
```

Expected output:

```
[info] KinesisRecordParserSpec
[info]   + parse returns an IrishEvent for valid JSON with all fields 1
[info]   + parse succeeds when startTime is absent (optional field) 2
[info]   + parse succeeds when coordinates are absent (optional field) 3
[info]   + parse returns None when a hard-required field (id) is missing 4
[info]   + parse succeeds when url is missing (optional field — defaults to empty string) 5
[info]   + parse succeeds when startDate is missing (optional field — defaults to empty string) 6
[info]   + parse succeeds with UNKNOWN county when county is not a recognised IrishCounty 7
[info]   + parse returns None for an empty JSON object 8
[info] Total 8, Failed 0, Passed 8
```

All 8 pass ✅

### Step 4o — Understand the test strategy

| Test | What it covers |
|---|---|
| 1. Full event | All fields present, including optional `startTime` and `coordinates` |
| 2. No startTime | Verifies optional string field is handled as `None` |
| 3. No coordinates | Verifies optional coordinates object is handled as `None` |
| 4. Missing hard-required field | Verifies `None` when a mandatory field (`id`) is absent |
| 5. Missing url | Verifies event is still parsed — `url` defaults to `""` |
| 6. Missing startDate | Verifies event is still parsed — `startDate` defaults to `""` |
| 7. Unknown county | Verifies event is still parsed — falls back to `IrishCounty.UNKNOWN` |
| 8. Empty object | Edge case — empty JSON returns `None` |

---

## Step 5: Create `EventStoreService.scala`

This service **composes** `KinesisRecordParser` with a save function. It processes a batch of JSON records and saves each one.

### Step 5a — Navigate

```
scala/src/main/scala/what/is/on/eire/
```

### Step 5b — Create the file

Create a new file named `EventStoreService.scala` in this directory.

### Step 5c — Package and imports

```scala
package what.is.on.eire

import cats.Applicative
import cats.syntax.functor._
import cats.syntax.traverse._
```

Press Enter twice.

### Step 5d — Add the Scaladoc comment

```scala
/** Processes a batch of JSON records (from a Kinesis trigger) and persists
  * each valid event via an injected save function.
  *
  * Takes a single function as a constructor parameter:
  *   - `saveEvent`: persists a single [[IrishEvent]] and returns a status message
  *
  * The `Applicative[F]` context bound allows us to use `.map` on `F` values
  * and lift pure values with `Applicative[F].pure`.
  * In production `F = IO`; in tests `F = IO` with mock functions.
  *
  * @tparam F the effect type (e.g. `IO`)
  * @param saveEvent persists a single event, returns a descriptive string
  */
```

### Step 5e — Class declaration

```scala
class EventStoreService[F[_]: Applicative](
  saveEvent: IrishEvent => F[String]
) {
```

Press Enter twice.

### Step 5f — Add the `processBatch` method

Indent 2 spaces inside the class, type:

```scala
/** Parse each JSON record and save the valid ones.
    *
    * @param jsonRecords a list of raw JSON strings, one per Kinesis record
    * @return a summary of how many were saved vs. failed to parse
    */
  def processBatch(jsonRecords: List[String]): F[String] = {
    val results = jsonRecords.map { json =>
      KinesisRecordParser.parse(json) match {
        case Some(event) =>
          saveEvent(event).map(savedMsg => s"SAVED: $savedMsg")
        case None        =>
          val preview = json.take(80).replace("\n", " ")
          Applicative[F].pure(s"SKIPPED: could not parse record: $preview")
      }
    }
    combineResults(results)
  }
```

### Step 5g — Add the `combineResults` helper

```scala
  /** Combine a list of F[String] results into a single F[String] summary. */
  private def combineResults(results: List[F[String]]): F[String] =
    results.sequence.map { msgs =>
      val saved   = msgs.count(_.startsWith("SAVED"))
      val skipped = msgs.count(_.startsWith("SKIPPED"))
      s"Processed ${msgs.size} records: $saved saved, $skipped skipped"
    }
```

### Step 5h — Close the class

Type `}` on a new line.

### Step 5i — Verify it compiles

```bash
sbt compile
```

Expected output: `[success]` with no errors.

### Step 5j — Walk through the design

```
┌──────────────────────────────────────────────────────┐
│         EventStoreService[F[_]: Applicative]         │
│                                                      │
│  processBatch(jsonRecords: List[String]):            │
│    F[String]                                         │
│                                                      │
│    1. For each JSON string:                          │
│       ├── KinesisRecordParser.parse(json)            │
│       │   → Some(event)  ──→ saveEvent(event).map    │
│       │   → None         ──→ Applicative[F].pure     │
│                                                      │
│    2. combineResults: results.sequence.map           │
│       → "X saved, Y skipped"                        │
└──────────────────────────────────────────────────────┘
```

**Design trade-offs:**

- **`[F[_]: Applicative]`** — The context bound gives us `.map` on `F` values and `Applicative[F].pure` for lifting pure values. This is the lightest constraint we need (we don't need `Monad`). In production `F = IO`; in tests `F = IO` with `IO.pure` mock functions.
- **`saveEvent: IrishEvent => F[String]`** — A single function means the service doesn't care about DynamoDB, JDBC, or any specific storage. It just calls the function.
- **`processBatch` returns a single summary `String`** — Rather than `List[F[Unit]]`, this makes it easy for the Lambda handler to log one line per batch.
- **No `pure()` helper** — We call `Applicative[F].pure(...)` directly inline. The old tutorial version had a separate helper with a confusing comment; this is cleaner.

---

## Step 6: Create `EventStoreServiceSpec.scala`

### Step 6a — Navigate

```
scala/src/test/scala/what/is/on/eire/
```

### Step 6b — Create the test file

Create a new file named `EventStoreServiceSpec.scala` in this directory.

### Step 6c — Package and imports

```scala
package what.is.on.eire

import weaver.SimpleIOSuite
import cats.effect.IO
```

Press Enter twice.

### Step 6d — Test class declaration

```scala
object EventStoreServiceSpec extends SimpleIOSuite {
```

### Step 6e — Create a shared helper event JSON

After the opening `{`, indent 2 spaces, type:

```scala
private val validJson: String =
    """{
      |  "id": "EVT001",
      |  "title": "Test Event",
      |  "url": "https://ticketmaster.ie/test",
      |  "startDate": "2026-06-15",
      |  "city": "Dublin",
      |  "county": "DUBLIN",
      |  "source": "Ticketmaster"
      |}""".stripMargin

  private val anotherValidJson: String =
    """{
      |  "id": "EVT002",
      |  "title": "Cork Festival",
      |  "url": "https://ticketmaster.ie/cork",
      |  "startDate": "2026-07-01",
      |  "city": "Cork",
      |  "county": "CORK",
      |  "source": "Ticketmaster"
      |}""".stripMargin
```

Press Enter twice.

### Step 6f — Test 1: batch with all valid records

```scala
test("processBatch saves all valid records and reports correct count") {
    var savedCount = 0

    val service = new EventStoreService[IO](
      saveEvent = event => IO {
        savedCount += 1
        s"${event.id} - ${event.title}"
      }
    )

    IO(service.processBatch(List(validJson, anotherValidJson))).map { result =>
      expect.all(
        result.contains("2 saved"),
        result.contains("0 skipped"),
        savedCount == 2
      )
    }
  }
```

Press Enter twice.

### Step 6g — Test 2: batch with mixed valid and invalid records

```scala
test("processBatch saves valid records and skips invalid ones") {
    val invalidJson = "{}"

    val service = new EventStoreService[IO](
      saveEvent = event => IO.pure(s"${event.id} saved")
    )

    IO(service.processBatch(List(validJson, invalidJson, anotherValidJson))).map { result =>
      expect.all(
        result.contains("2 saved"),
        result.contains("1 skipped")
      )
    }
  }
```

Press Enter twice.

### Step 6h — Test 3: batch with all invalid records

```scala
test("processBatch reports 0 saved when all records are invalid") {
    val invalid1 = "{}"
    val invalid2 = """{"bad": "data"}"""

    val service = new EventStoreService[IO](
      saveEvent = event => IO.pure(s"${event.id} saved")
    )

    IO(service.processBatch(List(invalid1, invalid2))).map { result =>
      expect.all(
        result.contains("0 saved"),
        result.contains("2 skipped")
      )
    }
  }
```

### Step 6i — Close the object

Type `}` on a new line.

### Step 6j — Run the tests

```bash
sbt "scalaModule/testOnly what.is.on.eire.EventStoreServiceSpec"
```

Expected output:

```
[info] EventStoreServiceSpec
[info]   + processBatch saves all valid records and reports correct count 1
[info]   + processBatch saves valid records and skips invalid ones 2
[info]   + processBatch reports 0 saved when all records are invalid 3
[info] Total 3, Failed 0, Passed 3
```

All 3 pass ✅

### Step 6k — Understand the test strategy

| Test | `saveEvent` behaviour | Input batch | Assertion |
|---|---|---|---|
| 1. All valid | Mutable counter + side-effect | 2 valid JSONs | `"2 saved, 0 skipped"` and counter == 2 |
| 2. Mixed | `IO.pure(...)` | 2 valid + 1 invalid | `"2 saved, 1 skipped"` |
| 3. All invalid | `IO.pure(...)` | 2 invalid | `"0 saved, 2 skipped"` |

---

## Step 7: Run All scala Module Tests Together

```bash
sbt "scalaModule/testOnly"
```

This runs all tests in the `scala` module:

```
[info] CityHeaderParserSpec
[info]   + ... 5 passed
[info] IngestionServiceSpec
[info]   + ... 3 passed
[info] KinesisRecordParserSpec
[info]   + ... 8 passed
[info] EventStoreServiceSpec
[info]   + ... 3 passed
[info] EventProcessorSpec
[info]   + ... 1 passed
[info] Total 20, Failed 0, Passed 20
```

All 20 tests pass. ✅

---

## Step 8: Create `DynamoDbEventRepository.scala`

This class lives in the `infra` module because it depends on the AWS DynamoDB SDK.

### Step 8a — Navigate

```
infra/src/main/scala/what/is/on/eire/
```

You should see:

```
HttpLambdaRunner.scala
IngestionLambdaHandler.scala
KinesisEventPublisher.scala
```

### Step 8b — Create the file

Create a new file named `DynamoDbEventRepository.scala` in this directory.

### Step 8c — Package and imports

```scala
package what.is.on.eire

import cats.effect.IO
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import java.util.HashMap
```

Press Enter twice.

### Step 8d — Add the Scaladoc comment

```scala
/** Persists [[IrishEvent]] records into the DynamoDB `irish-events` table.
  *
  * Uses the single-table design:
  *   - Partition Key (PK): `county_id` — e.g. "IE-D" for Dublin
  *   - Sort Key (SK):      `event_date_id` — e.g. "2026-08-15#Z7r9jZ1AdOkT9"
  *
  * This class is intentionally NOT unit-tested — it is a thin wrapper over the
  * AWS SDK. Test it via LocalStack integration tests or rely on the
  * [[EventStoreService]] unit tests which cover the orchestration logic.
  */
```

### Step 8e — Class declaration

```scala
class DynamoDbEventRepository(
  val client: DynamoDbClient,
  tableName: String
) {
```

### Step 8f — Add the `saveEvent` method

Indent 2 spaces inside the class, type:

```scala
/** Persist a single [[IrishEvent]] into DynamoDB.
    *
    * @param event the event to save
    * @return a descriptive string with the event ID and title
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
```

### Step 8g — Close the class

Type `}` on a new line.

### Step 8h — Add the resource companion

Outside the class, add:

```scala
object DynamoDbEventRepository {

  /** Creates a managed Resource that opens and closes the DynamoDB client.
    *
    * @param tableName the DynamoDB table name
    * @param localstackPort if set, connects to LocalStack instead of real AWS
    */
  def resource(
    tableName: String,
    localstackPort: Option[Int] = None
  ): cats.effect.Resource[IO, DynamoDbEventRepository] =
    cats.effect.Resource.make(
      acquire = IO {
        val builder = DynamoDbClient.builder
        localstackPort.foreach { port =>
          builder.endpointOverride(java.net.URI.create(s"http://localhost:$port"))
            .region(software.amazon.awssdk.regions.Region.EU_WEST_1)
            .credentialsProvider(
              software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("test", "test")
              )
            )
        }
        val client = builder.build
        new DynamoDbEventRepository(client, tableName)
      }
    )(release = repo => IO(repo.client.close()))
}
```

### Step 8i — Verify it compiles

```bash
sbt compile
```

Expected output: `[success]` with no errors.

---

## Step 9: Create `ProcessingLambdaHandler.scala`

This Lambda is triggered by a Kinesis Event Source Mapping. It receives a batch of Kinesis records and uses `EventStoreService` (with `DynamoDbEventRepository`) to persist them.

### Step 9a — Navigate

```
infra/src/main/scala/what/is/on/eire/
```

### Step 9b — Create the file

Create a new file named `ProcessingLambdaHandler.scala` in this directory.

### Step 9c — Package and imports

```scala
package what.is.on.eire

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._
```

Press Enter twice.

### Step 9d — Add the Scaladoc comment

```scala
/** Kinesis-triggered Lambda that processes raw event records and persists them
  * to DynamoDB.
  *
  * Uses `RequestHandler[KinesisEvent, String]` so AWS's Java runtime
  * deserialises the Kinesis event automatically — no manual serde needed.
  */
```

### Step 9e — Class declaration

```scala
class ProcessingLambdaHandler extends RequestHandler[KinesisEvent, String] {

  private given runtime: IORuntime = IORuntime.global

  private val tableName: String =
    Option(System.getenv("EVENTS_TABLE_NAME"))
      .getOrElse(throw new RuntimeException("EVENTS_TABLE_NAME env var not set"))
```

### Step 9f — Add the `handleRequest` method

Indent 2 spaces inside the class, type:

```scala
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
```

No `KinesisEventSerde` helper is needed — AWS's Java runtime handles the
JSON deserialisation automatically when you implement `RequestHandler[KinesisEvent, String]`.

### Step 9g — Verify it compiles

```bash
sbt compile
```

Expected output: `[success]` with no errors.

### Step 9h — Walk through the Lambda design

```
Kinesis Event Source Mapping
         │
         ▼
ProcessingLambdaHandler.handleRequest(event: KinesisEvent, ctx: Context): String
         │
         ├── 1. event.getRecords.asScala.toList      ← auto-deserialised by AWS runtime
         ├── 2. Decode each record's base64 data → JSON strings
         │
         ├── 3. DynamoDbEventRepository.resource(tableName).use { repo =>
         │       val service = new EventStoreService[IO](repo.saveEvent)
         │       service.processBatch(jsonRecords)
         │     }
         │
         └── 4. Return JSON string (AWS writes it to the response stream)
```

**Key design decisions:**

- **`RequestHandler[KinesisEvent, String]`** — AWS's Java runtime deserialises the incoming JSON into a `KinesisEvent` object automatically. No manual `InputStream` parsing or `KinesisEventSerde` needed. This is simpler than `RequestStreamHandler`.
- **Returns `String`** — The return value is the Lambda response. AWS writes it to the output stream for us. No `OutputStream` manipulation needed.
- **`DynamoDbEventRepository.resource(...)`** — Same resource pattern as `KinesisEventPublisher.resource(...)`. The client is created on invocation and closed after.
- **`EventStoreService[IO]`** — The pure service wired with the real DynamoDB function.

---

## Step 10: Create the Processing Sub-Stack

Following the same pattern as `KinesisSubStack`, `DynamoDbSubStack`, and `LambdasSubStack`, create a dedicated sub-stack for the Processing Lambda.

### Step 10a — Create `localstack/infra-cdk/lib/processing-substack.ts`

```typescript
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as path from 'path';

export interface ProcessingSubStackProps extends cdk.NestedStackProps {
  readonly rawStream: kinesis.IStream;
  readonly eventsTable: dynamodb.ITable;
}

export class ProcessingSubStack extends cdk.NestedStack {
  constructor(scope: Construct, id: string, props: ProcessingSubStackProps) {
    super(scope, id, props);

    const jarPath = path.join(
      __dirname,
      '../../../infra/target/scala-3.3.5/whats-on-eire-infra-assembly-0.1.0-SNAPSHOT.jar',
    );

    // Single Processing Lambda — reads from the shared Kinesis stream
    // and persists events to DynamoDB. Unlike the ingestion Lambdas,
    // there is only one — it handles all data sources.
    const processorFn = new lambda.Function(this, 'EventProcessor', {
      runtime: lambda.Runtime.JAVA_21,
      handler: 'what.is.on.eire.ProcessingLambdaHandler',
      code: lambda.Code.fromAsset(jarPath),
      timeout: cdk.Duration.seconds(30),
      memorySize: 512,
      environment: {
        EVENTS_TABLE_NAME: props.eventsTable.tableName,
      },
    });

    // Grant read access to the Kinesis stream
    props.rawStream.grantRead(processorFn);

    // Grant write access to the DynamoDB table
    props.eventsTable.grantWriteData(processorFn);

    // Connect the Kinesis stream as a trigger
    // Note: Using CfnEventSourceMapping (L1) instead of addEventSourceMapping (L2)
    // avoids tag propagation issues with LocalStack, which expects Tags as a
    // dict but CDK sends them as a list.
    new lambda.CfnEventSourceMapping(this, 'KinesisTrigger', {
      functionName: processorFn.functionName,
      eventSourceArn: props.rawStream.streamArn,
      startingPosition: 'TRIM_HORIZON',
      batchSize: 100,
    });

    new cdk.CfnOutput(this, 'ProcessorLambdaName', {
      value: processorFn.functionName,
      description: 'Processing Lambda function name',
    });
  }
}
```

### Step 10b — Wire it into `infra-cdk-stack.ts`

Add the import and instantiate it after the existing sub-stacks:

```typescript
import { ProcessingSubStack } from './processing-substack';

// Inside the constructor, after the existing sub-stacks:

// 4. Instantiate the Processing Sub-stack
new ProcessingSubStack(this, 'ProcessingSubStack', {
  rawStream: kinesisStack.rawStream,
  eventsTable: dynamoStack.eventsTable,
});
```

**LocalStack note:** If you have `cdk.Tags.of(this).add(...)` in the parent stack,
the tags propagate to the Event Source Mapping as a list, but LocalStack expects
a dict. Either comment out those tag lines for local deployment, or use
`CfnEventSourceMapping` (as shown above) to avoid the issue.

That's it — one sub-stack, one Lambda, not one per data source.

---

## Step 11: Verify End-to-End

### Step 11a — Rebuild the JAR

```bash
sbt "infra/assembly"
```

### Step 11b — Deploy infrastructure to LocalStack

```bash
cd localstack
./init-localstack.sh
```

This will:
1. Build the fat JAR
2. Delete any old Kinesis stream and DynamoDB table
3. Deploy the CDK stack (which includes the new Processing Lambda)
4. Verify both resources are ACTIVE

### Step 11c — Seed events into Kinesis via the Ingestion Lambda

```bash
curl -H "irish-location: Dublin" http://localhost:8080/event/pullEvents
```

Expected Lambda log output:

```
[Lambda] Requested city: Dublin
[Lambda] Fetched 20 events from Ticketmaster
[Lambda]   Published: Z7r9jZ1AdOkT9 - Test Event @ Dublin, DUBLIN
...
```

### Step 11d — Manually invoke the Processing Lambda

LocalStack Community Edition does **not** auto-trigger Lambda functions from Kinesis
Event Source Mappings (that's a Pro feature). The Lambda must be invoked manually.

First, find the Processing Lambda function name:

```bash
awslocal lambda list-functions --query "Functions[?contains(FunctionName, 'Processor')].[FunctionName]" --output text
```

Then invoke it with a sample Kinesis event payload. The payload wraps the
base64-encoded event JSON inside a `kinesis` record block:

```bash
awslocal lambda invoke \
  --function-name <paste-function-name-here> \
  --payload '{"Records":[{"kinesis":{"data":"eyJpZCI6InRlc3QwMDEiLCJ0aXRsZSI6IlRlc3QgRXZlbnQiLCJ1cmwiOiJodHRwczovL3Rlc3QuaWUiLCJzdGFydERhdGUiOiIyMDI2LTA2LTA1IiwiY2l0eSI6IkR1YmxpbiIsImNvdW50eSI6IkRVQkxJTiIsInNvdXJjZSI6IlRlc3QifQ==","partitionKey":"DUBLIN","kinesisSchemaVersion":"1.0"}}]}' \
  output.txt
```

If the Lambda succeeds, `output.txt` will contain `{"status": "SUCCESS", ...}`.

### Step 11e — Check the DynamoDB table

```bash
awslocal dynamodb scan --table-name irish-events
```

You should see items with `county_id`, `event_date_id`, `id`, `title`, etc.

Example output (abbreviated):

```json
{
    "Items": [
        {
            "county_id": {"S": "IE-D"},
            "event_date_id": {"S": "2026-06-19#17kZvxG61lYzm2"},
            "id": {"S": "17kZvxG61lYzm2"},
            "title": {"S": "Metallica: M72 World Tour"},
            "city": {"S": "Dublin"},
            "source": {"S": "Ticketmaster"}
        }
    ],
    "Count": 20
}
```

### Step 11f — Verify Processing Lambda logs

In production (or LocalStack Pro), the Event Source Mapping triggers automatically.
The Lambda logs would show:

```
WhatsOnEire Processing Lambda invoked!
Received 20 records from Kinesis stream
Processing complete: Processed 20 records: 20 saved, 0 skipped
```

---

## Step 12: Key Takeaways

### 12a — The architecture change

| Before | After |
|---|---|
| Events sit in Kinesis, unprocessed | Processing Lambda reads Kinesis and saves to DynamoDB |
| DynamoDB table exists but is empty | DynamoDB table populated with structured IrishEvent records |
| No consumer for the Kinesis stream | Kinesis Event Source Mapping triggers the Processing Lambda |

### 12b — The testability pattern (same as before)

```
class Service[F[_]](dependency: IrishEvent => F[String])
```

- The `EventStoreService` takes a **function** (`saveEvent: IrishEvent => F[String]`), not a concrete DynamoDB class
- Tests inject `IO.pure(...)` functions — no database, no I/O, instant assertions
- The parser is a pure `object` — tested without any effect system at all

### 12c — Separation of concerns

| Concern | Handled by | Module |
|---|---|---|
| Parsing Kinesis JSON | `KinesisRecordParser` | `scala` (pure) |
| Routing/Orchestration | `EventStoreService` | `scala` (pure) |
| DynamoDB putItem | `DynamoDbEventRepository` | `infra` (AWS SDK) |
| Lambda entry point | `ProcessingLambdaHandler` | `infra` (AWS Lambda) |
| Infrastructure provisioning | CDK TypeScript | `infra-cdk` |

### 12d — When new data sources are added

The `EventStoreService` doesn't care about data sources. It processes whatever JSON arrives on the Kinesis stream. When Meetup, FailteIreland, etc. are added:

- Their ingestion Lambdas publish to the **same** `events-raw-stream`
- The `ProcessingLambdaHandler` processes **all** of them transparently
- No changes needed to the processing pipeline — it's fully data-source-agnostic

---

## Appendix: File Map

### New files

```
scala/src/main/scala/what/is/on/eire/
├── KinesisRecordParser.scala    ← Pure JSON ⇒ IrishEvent parsing (regex-based)
└── EventStoreService.scala      ← Orchestrates parsing + saving via injected function

scala/src/test/scala/what/is/on/eire/
├── KinesisRecordParserSpec.scala  ← 8 tests for the parser
└── EventStoreServiceSpec.scala    ← 3 tests for the service

infra/src/main/scala/what/is/on/eire/
├── DynamoDbEventRepository.scala  ← DynamoDB putItem implementation
└── ProcessingLambdaHandler.scala  ← Kinesis-triggered Lambda
```

### Modified file

```
build.sbt  ← Added dynamodb SDK dependency
```

### Infrastructure (deploy separately from code)

The Processing Lambda is a single Lambda deployed alongside the existing infrastructure.
Unlike the ingestion Lambdas (one per data source), only one Processing Lambda is
needed — it reads from the shared `events-raw-stream` and handles all data sources.

### Unchanged files

```
infra/src/main/scala/what/is/on/eire/
├── HttpLambdaRunner.scala       ← No changes needed
├── IngestionLambdaHandler.scala ← No changes needed
└── KinesisEventPublisher.scala  ← No changes needed (it writes the JSON we parse)

scala/src/main/scala/what/is/on/eire/
├── CityHeaderParser.scala       ← No changes needed
├── EventProcessor.scala         ← No changes needed
├── IngestionService.scala       ← No changes needed
├── Main.scala                   ← No changes needed
├── MainService.scala            ← No changes needed
└── TicketmasterClient.scala     ← No changes needed
```
