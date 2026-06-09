package what.is.on.eire

/** Fallback enricher used when no source‑specific enricher is registered.
  *
  * Passes core fields through and leaves all rich fields empty.
  */
class BasicEnricher extends EventEnricher {

  override val source: String = ""

  override def enrich(event: IrishEvent): EnrichedEvent =
    EnrichedEvent(
      id = event.id,
      title = event.title,
      url = event.url,
      startDate = event.startDate,
      startTime = event.startTime,
      endDate = Some(event.startDate),
      timezone = None,
      status = None,
      city = event.city,
      county = Some(event.county),
      coordinates = event.coordinates,
      source = event.source,
      images = None,
      venueName = None,
      venueAddress = None,
      genre = None,
      subGenre = None,
      artistNames = None,
      artistLinks = None,
      description = None,
      salesStart = None,
      salesEnd = None
    )
}
