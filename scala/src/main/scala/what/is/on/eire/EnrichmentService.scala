package what.is.on.eire

/** Dispatches [[IrishEvent]] enrichment to the correct source‑specific [[EventEnricher]].
  *
  * Adding a new API:
  *   1. Implement [[EventEnricher]] 2. Pass it to the constructor
  *
  * This class never changes when a new source arrives — it's closed for modification, open for
  * extension via the [[EventEnricher]] trait.
  */
class EnrichmentService(enrichers: List[EventEnricher]) {

  private val bySource: Map[String, EventEnricher] =
    enrichers.map(e => e.source -> e).toMap

  private val fallback: EventEnricher = new BasicEnricher

  def enrich(event: IrishEvent): EnrichedEvent =
    bySource.getOrElse(event.source, fallback).enrich(event)
}
