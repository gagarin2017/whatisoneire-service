package what.is.on.eire

/** Pure utility that extracts the `irish-location` header value from a raw Lambda invocation
  * payload.
  *
  * Uses substring matching (`findFirstMatchIn`) rather than pattern-match (`unapplySeq`) because
  * the payload contains surrounding JSON structure.
  *
  * Thread-safe: the regex is compiled once and shared across all calls.
  */
object CityHeaderParser {

  private val headerRegex = """irish-location"\s*:\s*"([^"]+)"""".r

  def parse(rawInput: String): Option[String] =
    headerRegex.findFirstMatchIn(rawInput).map(_.group(1))

}
