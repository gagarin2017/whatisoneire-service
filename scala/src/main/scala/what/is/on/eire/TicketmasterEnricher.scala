package what.is.on.eire

import java.nio.charset.StandardCharsets
import smithy4s.Blob
import smithy4s.Schema
import smithy4s.json.Json

/** Extracts rich fields from Ticketmaster's raw event JSON. */
class TicketmasterEnricher extends EventEnricher {

  override val source: String = "Ticketmaster"

  override def enrich(event: IrishEvent): EnrichedEvent = {
    val tmEvent = parsePayload[TicketmasterEvent](event.rawPayload)
    val venue   = tmEvent.flatMap(e => e._embedded.venues.flatMap(_.headOption))
    val attract = tmEvent.flatMap(e => e._embedded.attractions.flatMap(_.headOption))
    val clsf    = tmEvent.flatMap(e => e.classifications.flatMap(_.headOption))
    val dates   = tmEvent.map(e => e.dates)

    EnrichedEvent(
      // ── Core (pass‑through) ──
      id = event.id,
      title = event.title,
      url = event.url,
      startDate = event.startDate,
      startTime = event.startTime,
      endDate = dates.flatMap(d => d.end.flatMap(_.localDate)).orElse(Some(event.startDate)),
      timezone = dates.flatMap(_.timezone),
      status = dates.flatMap(_.status).flatMap(_.code),
      city = event.city,
      county = Some(event.county),
      coordinates = event.coordinates,
      source = event.source,

      // ── Rich (extracted) ──
      images = tmEvent.flatMap(toEnrichedImages),
      venueName = venue.flatMap(_.name),
      venueAddress = venue.flatMap(v => v.address.flatMap(_.line1)),
      genre = clsf.flatMap(c => c.segment.map(_.name)),
      subGenre = clsf.flatMap(c => c.subGenre.map(_.name)),
      artistNames = tmEvent.map(e => e._embedded.attractions.getOrElse(Nil).map(_.name)),
      artistLinks = attract.map(a => extractArtistLinks(a.externalLinks)),
      description = tmEvent.flatMap(_.info),
      salesStart = tmEvent.flatMap(e => e.sales.flatMap(s => s.public.startDateTime)),
      salesEnd = tmEvent.flatMap(e => e.sales.flatMap(s => s.public.endDateTime))
    )
  }

  // ── Private helpers ─────────────────────────────────────────────────────

  private def parsePayload[T: Schema](json: String): Option[T] = {
    val blob    = Blob(json.getBytes(StandardCharsets.UTF_8))
    val decoder = Json.payloadCodecs.decoders.fromSchema(Schema[T])
    decoder.decode(blob).toOption
  }

  private def toEnrichedImages(event: TicketmasterEvent): Option[List[EnrichedImage]] =
    event.images.map { imgs =>
      imgs.map(im =>
        EnrichedImage(url = im.url, ratio = im.ratio, width = im.width, height = im.height)
      )
    }

  private def firstLink(links: Option[List[TicketmasterLink]]): Option[String] =
    links.flatMap(_.headOption).flatMap(_.url)

  private def extractArtistLinks(links: Option[TicketmasterExternalLinks]): ArtistLinks =
    links match {
      case Some(l) =>
        ArtistLinks(
          spotify = firstLink(l.spotify),
          youtube = firstLink(l.youtube),
          instagram = firstLink(l.instagram),
          twitter = firstLink(l.twitter),
          facebook = firstLink(l.facebook),
          website = firstLink(l.homepage),
          wikipedia = firstLink(l.wiki),
          tiktok = firstLink(l.tiktok)
        )
      case None    => emptyArtistLinks
    }

  private val emptyArtistLinks =
    ArtistLinks(None, None, None, None, None, None, None, None)
}
