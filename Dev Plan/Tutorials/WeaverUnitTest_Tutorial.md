# Tutorial: Extract IngestionService & Write Weaver Unit Tests

## Goal

Refactor the `IngestionLambdaHandler` so it delegates **all business logic** to a pure `IngestionService` in the `scala` module. The Lambda handler becomes **pure infra** — create clients, call the service, push to Kinesis.

We create two new files:

```
scala/src/main/scala/what/is/on/eire/
├── CityHeaderParser.scala      ← NEW: extracts city from raw JSON payload
└── IngestionService.scala      ← NEW: composes parsing + API dispatch

scala/src/test/scala/what/is/on/eire/
├── CityHeaderParserSpec.scala  ← NEW: 5 Weaver test cases
└── IngestionServiceSpec.scala  ← NEW: 3 Weaver test cases
```

And update one existing file:

```
infra/src/main/scala/what/is/on/eire/
└── IngestionLambdaHandler.scala  ← UPDATED: use the service instead of inline logic
```

---

## Prerequisites

- `sbt compile` works
- You've read `System_Architecture_Pipeline.md`
- You understand the regex bug (Scala `match` requires full-string, `findFirstMatchIn` does substring)

---

## Step 0: Understand the Architecture

Before writing code, let's understand where everything lives and why.

### Current flow (problematic)

```
HttpLambdaRunner                  IngestionLambdaHandler
      │                                   │
      ├── extracts header                 ├── regex-parses city from JSON
      ├── serializes to JSON ──→ Lambda ──→── creates http4s client
      │                                   ├── creates TicketmasterClient
      │                                   ├── calls getEvents or getAllEvents
      │                                   └── publishes to Kinesis
```

The Lambda handler does **both** business logic (parsing, routing) AND infrastructure (clients, Kinesis). This makes it hard to test and violates separation of concerns.

### Target flow

```
HttpLambdaRunner                  IngestionLambdaHandler
      │                                   │
      ├── extracts header                 ├── creates http4s client
      ├── serializes to JSON ──→ Lambda ──→── creates TicketmasterClient
      │                                   │
      │                                   ├── calls IngestionService
      │                                   │   .fetchEvents(rawInput)
      │                                   │        │
      │                                   │        ├── CityHeaderParser
      │                                   │        │   .parse(rawInput)
      │                                   │        │     → Some("Dublin")
      │                                   │        │
      │                                   │        └── calls the right
      │                                   │            TicketmasterClient
      │                                   │            method based on city
      │                                   │
      │                                   └── publishes results to Kinesis
```

The **Lambda handler** only does:
1. Create AWS/http4s clients (infra)
2. Call `IngestionService.fetchEvents(rawInput)` (delegate to domain)
3. Push results to Kinesis (infra)

The **`IngestionService`** only does:
1. Call `CityHeaderParser.parse(rawInput)` to get the city
2. Route to `fetchByCity(city)` or `fetchAll` based on the result

Both `CityHeaderParser` and `IngestionService` live in the `scala` module — pure Scala, no AWS imports, easily testable with Weaver.

---

## Step 1: Create `CityHeaderParser.scala`

### Step 1a — Navigate

Open the directory:

```
scala/src/main/scala/what/is/on/eire/
```

List its contents to confirm you're in the right place. You should see:

```
EventProcessor.scala
Main.scala
MainService.scala
TicketmasterClient.scala
```

### Step 1b — Create the file

Create a new file named `CityHeaderParser.scala` in this directory.

### Step 1c — Add the package declaration

Type this as the first line:

```scala
package what.is.on.eire
```

Press Enter twice.

### Step 1d — Add the Scaladoc comment

Type:

```scala
/** Pure utility that extracts the `irish-location` header value from a raw
  * Lambda invocation payload.
  *
  * Uses substring matching (`findFirstMatchIn`) rather than pattern-match
  * (`unapplySeq`) because the payload contains surrounding JSON structure.
  *
  * Thread-safe: the regex is compiled once and shared across all calls.
  */
```

### Step 1e — Add the object declaration

Type:

```scala
object CityHeaderParser {
```

### Step 1f — Add the regex

Indent 2 spaces inside the object, type:

```scala
private val headerRegex = """irish-location"\s*:\s*"([^"]+)"""".r
```

Press Enter.

### Step 1g — Add the `parse` method

Type:

```scala
def parse(rawInput: String): Option[String] =
    headerRegex.findFirstMatchIn(rawInput).map(_.group(1))
```

### Step 1h — Close the object

Type `}` on a new line.

### Step 1i — Verify it compiles

Run in your terminal:

```bash
sbt compile
```

Expected output: `[success]` with no errors.

### Step 1j — Walk through what you just wrote

| Line | What it does |
|---|---|
| `object CityHeaderParser` | Singleton — no mutable state, thread-safe |
| `private val headerRegex` | Regex compiled **once** when the object loads, not on every `parse()` call |
| `"""irish-location"\s*:\s*"([^"]+)""".r` | The pattern: matches `irish-location": "Dublin"` and captures the city name in group 1 |
| `.findFirstMatchIn(rawInput)` | Searches for the pattern as a **substring** anywhere in `rawInput` |
| `.map(_.group(1))` | If found, extracts the first capture group (the city) wrapped in `Option` |

---

## Step 2: Create `CityHeaderParserSpec.scala`

### Step 2a — Navigate

Open the test directory:

```
scala/src/test/scala/what/is/on/eire/
```

List its contents. You should see:

```
EventProcessorSpec.scala
resource/
```

### Step 2b — Create the test file

Create a new file named `CityHeaderParserSpec.scala` in this directory.

### Step 2c — Package and imports

Type:

```scala
package what.is.on.eire

import weaver.SimpleIOSuite
import cats.effect.IO
```

Press Enter twice.

### Step 2d — Test class declaration

Type:

```scala
object CityHeaderParserSpec extends SimpleIOSuite {
```

### Step 2e — Test 1: happy path

Indent 2 spaces, type:

```scala
test("parse returns Some(city) when irish-location header is present") {
    val payload =
      """{
        |  "headers": { "irish-location": "Dublin" },
        |  "body": "{}"
        |}""".stripMargin

    IO(CityHeaderParser.parse(payload)).map { result =>
      expect(result == Some("Dublin"))
    }
  }
```

Press Enter twice.

### Step 2f — Test 2: different city

Type:

```scala
test("parse returns Some(city) for a different city") {
    val payload =
      """{
        |  "headers": { "irish-location": "Galway" },
        |  "body": "{}"
        |}""".stripMargin

    IO(CityHeaderParser.parse(payload)).map { result =>
      expect(result == Some("Galway"))
    }
  }
```

Press Enter twice.

### Step 2g — Test 3: special characters

Type:

```scala
test("parse returns Some(city) for a city with special characters") {
    val payload =
      """{
        |  "headers": { "irish-location": "Dún Laoghaire" },
        |  "body": "{}"
        |}""".stripMargin

    IO(CityHeaderParser.parse(payload)).map { result =>
      expect(result == Some("Dún Laoghaire"))
    }
  }
```

Press Enter twice.

### Step 2h — Test 4: no header (cron trigger)

Type:

```scala
test("parse returns None when irish-location header is absent") {
    val payload =
      """{
        |  "body": "{}"
        |}""".stripMargin

    IO(CityHeaderParser.parse(payload)).map { result =>
      expect(result == None)
    }
  }
```

Press Enter twice.

### Step 2i — Test 5: empty payload

Type:

```scala
test("parse returns None for an empty payload") {
    val payload = "{}"

    IO(CityHeaderParser.parse(payload)).map { result =>
      expect(result == None)
    }
  }
```

### Step 2j — Close the object

Type `}` on a new line.

### Step 2k — Run the tests

```bash
sbt "scalaModule/testOnly what.is.on.eire.CityHeaderParserSpec"
```

Expected output:

```
[info] CityHeaderParserSpec
[info]   + parse returns Some(city) when irish-location header is present 1
[info]   + parse returns Some(city) for a different city 2
[info]   + parse returns Some(city) for a city with special characters 3
[info]   + parse returns None when irish-location header is absent 4
[info]   + parse returns None for an empty payload 5
[info] Total 5, Failed 0, Passed 5
```

All 5 pass ✅

### Step 2l — Understand the test structure

| Piece | Purpose |
|---|---|
| `object CityHeaderParserSpec` | Test class — must extend a Weaver suite trait |
| `extends SimpleIOSuite` | Weaver's base trait for tests that return `IO[...]` |
| `test("description") { ... }` | Each named test is a block inside the object |
| `IO(...).map { result => expect(...) }` | Weaver wraps assertions in `IO` and uses `expect(...)` which returns `Expectations` |

### Step 2m — (Optional) Verify the test catches the old bug

Temporarily change `CityHeaderParser.parse` to use the broken pattern-match:

```scala
// BROKEN — for demonstration only
def parse(rawInput: String): Option[String] =
  rawInput match {
    case headerRegex(city) => Some(city)
    case _                 => None
  }
```

Re-run the tests:

```bash
sbt "scalaModule/testOnly what.is.on.eire.CityHeaderParserSpec"
```

Tests 1, 2, 3 should now fail (they get `None` instead of `Some(city)`). Tests 4 and 5 still pass (they expect `None`).

**Revert the broken change** before proceeding.

---

## Step 3: Create `IngestionService.scala`

Now we create the service that **composes** the parser with the API call. It takes two functions as constructor parameters — this is what makes it fully testable without any real API client.

### Step 3a — Navigate

```
scala/src/main/scala/what/is/on/eire/
```

### Step 3b — Create the file

Create a new file named `IngestionService.scala` in this directory.

### Step 3c — Package and imports

Type:

```scala
package what.is.on.eire
```

Press Enter twice.

### Step 3d — Add the Scaladoc comment

Type:

```scala
/** Orchestrates the ingestion flow for a single data source.
  *
  * Takes two pure functions as constructor parameters:
  *   - `fetchByCity`: fetches events filtered by a specific city
  *   - `fetchAll`:    fetches all events (no city filter, used by cron trigger)
  *
  * This design makes the service fully testable — pass simple `IO.pure(...)`
  * functions in tests instead of real API clients.
  *
  * When new data sources are added (Meetup, FailteIreland, etc.),
  * compose their fetch functions into the same interface.
  *
  * @tparam F the effect type (e.g. `IO`, `cats.Id` in tests)
  * @param fetchByCity fetches events for a given city name
  * @param fetchAll    fetches all events (cron trigger, no filter)
  */
```

### Step 3e — Class declaration

Type:

```scala
class IngestionService[F[_]](
  fetchByCity: String => F[List[IrishEvent]],
  fetchAll: F[List[IrishEvent]]
) {
```

### Step 3f — Add the `fetchEvents` method

Indent 2 spaces inside the class, type:

```scala
/** Parse the `irish-location` header from the raw Lambda input and
    * dispatch to the right fetch function.
    *
    * @param rawInput the full Lambda invocation payload as a JSON string
    * @return the list of events wrapped in the effect type `F`
    */
  def fetchEvents(rawInput: String): F[List[IrishEvent]] = {
    val requestedCity = CityHeaderParser.parse(rawInput)
    requestedCity match {
      case Some(city) => fetchByCity(city)
      case None       => fetchAll
    }
  }
```

### Step 3g — Close the class

Type `}` on a new line.

### Step 3h — Verify it compiles

```bash
sbt compile
```

### Step 3i — Walk through the design

```
                 ┌────────────────────────────────────────┐
                 │          IngestionService[F]            │
                 │                                        │
                 │  fetchEvents(rawInput: String):         │
                 │    F[List[IrishEvent]]                  │
                 │                                        │
                 │    1. CityHeaderParser.parse(rawInput)  │
                 │       → Some("Dublin") or None          │
                 │                                        │
                 │    2. Match on result:                  │
                 │       Some(city) → fetchByCity(city)    │
                 │       None        → fetchAll            │
                 └────────────────────────────────────────┘
```

Why is `IngestionService` parameterized on `F`?

- In **production**, `F = IO` — the functions call the real Ticketmaster API
- In **tests**, `F = IO` with mock functions — no HTTP calls, instant assertions
- **No `Functor` constraint needed** — the service just calls the functions and returns the result, it never transforms the `F` value

Why take functions as constructor parameters instead of a `TicketmasterClient`?

- **Testability without smithy4s**: `TicketmasterClient` requires a `TicketmasterApi[F]` which is generated by smithy4s. Creating one in a test requires the full codegen pipeline. Functions are trivial to provide.
- **Future-proofing**: When we add Meetup, FailteIreland, etc., we can compose multiple data sources behind these same two function parameters. The service never changes.

---

## Step 4: Create `IngestionServiceSpec.scala`

### Step 4a — Navigate

```
scala/src/test/scala/what/is/on/eire/
```

### Step 4b — Create the test file

Create a new file named `IngestionServiceSpec.scala` in this directory.

### Step 4c — Package and imports

Type:

```scala
package what.is.on.eire

import weaver.SimpleIOSuite
import cats.effect.IO
```

Press Enter twice.

### Step 4d — Test class declaration

Type:

```scala
object IngestionServiceSpec extends SimpleIOSuite {
```

### Step 4e — Create a shared helper event

After the opening `{`, indent 2 spaces, type:

```scala
private val sampleEvent = IrishEvent(
    id = "Z7r9jZ1AdOkT9",
    title = "Test Event",
    url = "https://www.ticketmaster.ie/test",
    startDate = "2026-06-15",
    city = "Dublin",
    county = IrishCounty.DUBLIN,
    source = "Ticketmaster",
    startTime = None,
    coordinates = None
  )
```

Press Enter twice.

### Step 4f — Test 1: city present → calls fetchByCity

Type:

```scala
test("fetchEvents calls fetchByCity when irish-location header is present") {
    val payload =
      """{
        |  "headers": { "irish-location": "Cork" },
        |  "body": "{}"
        |}""".stripMargin

    val service = new IngestionService[IO](
      fetchByCity = city => IO.pure(List(sampleEvent.copy(city = city))),
      fetchAll    = IO.pure(List(sampleEvent))
    )

    IO(service.fetchEvents(payload)).map { result =>
      expect.all(
        result.size == 1,
        result.head.city == "Cork"
      )
    }
  }
```

Press Enter twice.

### Step 4g — Test 2: no header → calls fetchAll

Type:

```scala
test("fetchEvents calls fetchAll when irish-location header is absent") {
    val payload =
      """{
        |  "body": "{}"
        |}""".stripMargin

    val service = new IngestionService[IO](
      fetchByCity = city => IO.pure(List(sampleEvent.copy(city = city))),
      fetchAll    = IO.pure(List(sampleEvent))
    )

    IO(service.fetchEvents(payload)).map { result =>
      expect.all(
        result.size == 1,
        result.head.city == "Dublin"
      )
    }
  }
```

Press Enter twice.

### Step 4h — Test 3: empty payload → calls fetchAll

Type:

```scala
test("fetchEvents calls fetchAll for an empty payload") {
    val payload = "{}"

    val service = new IngestionService[IO](
      fetchByCity = city => IO.pure(List(sampleEvent.copy(city = city))),
      fetchAll    = IO.pure(Nil)
    )

    IO(service.fetchEvents(payload)).map { result =>
      expect(result == Nil)
    }
  }
```

### Step 4i — Close the object

Type `}` on a new line.

### Step 4j — Run the tests

```bash
sbt "scalaModule/testOnly what.is.on.eire.IngestionServiceSpec"
```

Expected output:

```
[info] IngestionServiceSpec
[info]   + fetchEvents calls fetchByCity when irish-location header is present 1
[info]   + fetchEvents calls fetchAll when irish-location header is absent 2
[info]   + fetchEvents calls fetchAll for an empty payload 3
[info] Total 3, Failed 0, Passed 3
```

All 3 pass ✅

### Step 4k — Understand the test strategy

Since `IngestionService` takes **functions** as constructor parameters, we can inject lightweight `IO.pure(...)` functions in tests — no real HTTP calls, no smithy4s, no Ticketmaster API key needed.

| Test | `fetchByCity` returns | `fetchAll` returns | What we assert |
|---|---|---|---|
| City "Cork" present | `List(event.copy(city="Cork"))` | `List(event)` | Result has 1 event with `city == "Cork"` |
| No header (cron) | `List(event.copy(city=city))` | `List(event)` | Result has 1 event with `city == "Dublin"` (from sampleEvent) |
| Empty payload | `List(event.copy(city=city))` | `Nil` | Result is an empty list |

Test 3 is important — it ensures the **cron path** was taken (returned `Nil` from `fetchAll`), not the city path (which would have returned a non-empty list).

---

## Step 5: Run All Tests Together

```bash
sbt "scalaModule/testOnly"
```

This runs all tests in the `scala` module — `CityHeaderParserSpec`, `IngestionServiceSpec`, and the existing `EventProcessorSpec`.

Expected output:

```
[info] CityHeaderParserSpec
[info]   + ... 5 passed
[info] IngestionServiceSpec
[info]   + ... 3 passed
[info] EventProcessorSpec
[info]   + ... 1 passed
[info] Total 9, Failed 0, Passed 9
```

All 9 tests pass. ✅

---

## Step 6: Update `IngestionLambdaHandler.scala`

Now wire the new service into the Lambda handler. The handler becomes a thin infra wrapper.

### Step 6a — Navigate

```
infra/src/main/scala/what/is/on/eire/
```

### Step 6b — Open `IngestionLambdaHandler.scala`

### Step 6c — Find the city-parsing section

Locate these lines (around line 30-37):

```scala
    // ── Parse the irish-location header from the incoming payload ──
    // Supports both: { "headers": { "irish-location": "Dublin" } }
    // and:           { "headers": { "irish-location": "Dublin" } }
    val requestedCity: Option[String] = {
      val headerRegex = """irish-location"\s*:\s*"([^"]+)"""".r
      headerRegex.findFirstMatchIn(rawInput).map(_.group(1))
    }
    logger.log(s"Requested city: ${requestedCity.getOrElse("ALL (cron)")}")
```

### Step 6d — Replace with the service call

Delete lines from `// ── Parse the irish-location` through `logger.log(s"Requested city: ...")` and replace with:

```scala
    // ── Parse the irish-location header and fetch events ──
    val service = new IngestionService[IO](
      fetchByCity = city => tmClient.getEvents(city),
      fetchAll    = tmClient.getAllEvents
    )
```

### Step 6e — Find the `for` comprehension that uses `requestedCity`

Locate around line 47-78, the `yield` block that contains:

```scala
    } yield {
      val tmClient = new TicketmasterClient[cats.effect.IO](tmApi, apiKey)

      // Fetch events — filtered by city if requested, otherwise all
      val fetch = requestedCity match {
        case Some(city) => tmClient.getEvents(city)
        case None       => tmClient.getAllEvents
      }

      val events = fetch.unsafeRunSync()
      val label  = requestedCity.getOrElse("all Ireland")
      logger.log(s"Fetched ${events.size} events from Ticketmaster for $label")

      events.foreach { e =>
        publisher.publish(e).unsafeRunSync()
        logger.log(s"  Published: ${e.id} - ${e.title} @ ${e.city}, ${e.county}")
      }

      events // return so the response includes the count
    }
```

### Step 6f — Replace with simplified yield block

Replace the entire `yield` block (from `} yield {` to the closing `}`) with:

```scala
    } yield {
      val tmClient = new TicketmasterClient[cats.effect.IO](tmApi, apiKey)

      val service = new IngestionService[IO](
        fetchByCity = city => tmClient.getEvents(city),
        fetchAll    = tmClient.getAllEvents
      )

      val events = service.fetchEvents(rawInput).unsafeRunSync()
      logger.log(s"Fetched ${events.size} events from Ticketmaster")

      events.foreach { e =>
        publisher.publish(e).unsafeRunSync()
        logger.log(s"  Published: ${e.id} - ${e.title} @ ${e.city}, ${e.county}")
      }

      events
    }
```

### Step 6g — Remove the duplicate service creation

Check that the `service` is created **only once** (inside the `for`/`yield`). If you created it outside the `yield` in Step 6d, remove that duplicate. The final `yield` block should be the only place `IngestionService` is instantiated.

### Step 6h — Verify the handler compiles

```bash
sbt compile
```

### Step 6i — Verify with all tests

```bash
sbt test
```

All tests in `scala` module should still pass. The `infra` module has no tests yet.

---

## Step 7: Verify End-to-End

### Step 7a — Rebuild the JAR

```bash
sbt "infra/assembly"
```

### Step 7b — Restart HttpLambdaRunner

```bash
export TICKETMASTER_API_KEY=your_key_here
sbt "infra/runMain what.is.on.eire.HttpLambdaRunner"
```

### Step 7c — Test with a city header

```bash
curl -H "irish-location: Dublin" http://localhost:8080/event/pullEvents
```

Lambda log should show:

```
[Lambda] Requested city: Dublin
[Lambda] Fetched 87 events from Ticketmaster
[Lambda]   Published: Z7r9jZ1AdOkT9 - Test Event @ Dublin, DUBLIN
...
```

Instead of the old buggy:

```
[Lambda] Requested city: ALL (cron)
```

### Step 7d — Test the cron trigger

```bash
curl http://localhost:8080/cron/trigger
```

Lambda log should show:

```
[Lambda] Requested city: ALL (cron)
[Lambda] Fetched 150 events from Ticketmaster
```

---

## Step 8: Key Takeaways

### 8a — The architecture change

| Before | After |
|---|---|
| Lambda handler parses city inline | `CityHeaderParser.parse()` in `scala` module |
| Lambda handler routes city/API logic | `IngestionService.fetchEvents()` in `scala` module |
| Business logic mixed with AWS imports | Business logic is pure, no AWS imports |
| Not testable (regex hidden in Lambda) | Testable with Weaver + `IO.pure` mocks |
| Adding new data source changes Lambda | Adding new data source changes only the function passed to service |

### 8b — The testability pattern

```
class Service[F[_]](dependency: Param => F[Result])
```

- Dependencies are **functions**, not concrete classes
- `F` abstracts over the effect type (`IO` in prod, `IO` or `Id` in tests)
- Tests inject `(x => IO.pure(expected))` — no real I/O, instant assertions

### 8c — When new data sources are added

When Meetup, FailteIreland, etc. are added:

```scala
// Compose multiple sources into the same function interface
val fetchByCity = (city: String) => for {
  tm  <- tmClient.getEvents(city)
  m   <- meetupClient.getEvents(city)
  fi  <- failteClient.getEvents(city)
} yield tm ++ m ++ fi

val fetchAll = for {
  tm  <- tmClient.getAllEvents
  m   <- meetupClient.getAllEvents
  fi  <- failteClient.getAllEvents
} yield tm ++ m ++ fi

val service = new IngestionService[IO](fetchByCity, fetchAll)
```

The `IngestionService` class never changes. Neither do its tests.

---

## Appendix: File Map

### New files

```
scala/src/main/scala/what/is/on/eire/
├── CityHeaderParser.scala      ← Pure regex: String => Option[String]
└── IngestionService.scala      ← Composes parser + fetch functions

scala/src/test/scala/what/is/on/eire/
├── CityHeaderParserSpec.scala  ← 5 tests for the parser
└── IngestionServiceSpec.scala  ← 3 tests for the service
```

### Modified file

```
infra/src/main/scala/what/is/on/eire/
└── IngestionLambdaHandler.scala  ← Uses IngestionService instead of inline logic
```

### Unchanged files

```
infra/src/main/scala/what/is/on/eire/
├── HttpLambdaRunner.scala       ← No changes needed
├── KinesisEventPublisher.scala  ← No changes needed
└── LocalStackRunner.scala       ← No changes needed

scala/src/main/scala/what/is/on/eire/
├── EventProcessor.scala         ← No changes needed
├── Main.scala                   ← No changes needed
├── MainService.scala            ← No changes needed
└── TicketmasterClient.scala     ← No changes needed
```
