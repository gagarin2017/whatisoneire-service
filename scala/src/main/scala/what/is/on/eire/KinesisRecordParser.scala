package what.is.on.eire

/** Pure utility that deserialises a JSON string (from a single Kinesis record) into an
  * [[IrishEvent]] case class.
  *
  * Expects the JSON shape produced by [[KinesisEventPublisher.serializeEvent]]. Uses minimal string
  * parsing — no JSON library dependency.
  *
  * Only `id`, `title`, `city`, `county`, and `source` are hard-required for a successful parse.
  * Fields `url`, `startDate`, `startTime`, and `coordinates` are optional and default to empty
  * string or `None` when absent. The `county` field tolerates any string value — unrecognised
  * values fall back to [[IrishCounty.UNKNOWN]]. This ensures events are saved to the database even
  * if the source data is incomplete.
  *
  * Thread-safe: all parsing is stateless.
  */
object KinesisRecordParser {

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

  /** Parse a JSON county string back into an [[IrishCounty]] enum value. Unrecognised county
    * strings fall back to [[IrishCounty.UNKNOWN]] so the event is still saved to the database.
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

}
