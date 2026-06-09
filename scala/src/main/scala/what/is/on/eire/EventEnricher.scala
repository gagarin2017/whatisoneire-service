package what.is.on.eire

/** Contract for enriching an [[IrishEvent]] into an [[EnrichedEvent]] from a specific data source.
  *
  * Implement one per API (Ticketmaster, Eventbrite, Ents24, etc.) and register it with
  * [[EnrichmentService]]. The service dispatches by matching [[IrishEvent.source]] to [[source]].
  */
trait EventEnricher {

  /** The value that matches [[IrishEvent.source]] for this enricher (e.g. "Ticketmaster"). */
  def source: String

  /** Produce a fully enriched [[EnrichedEvent]] from a raw [[IrishEvent]]. */
  def enrich(event: IrishEvent): EnrichedEvent
}
