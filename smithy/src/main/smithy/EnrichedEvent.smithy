$version: "2"

namespace what.is.on.eire

/// A fully‑normalised event view for the frontend.
///
/// Built at read‑time by enriching an [[IrishEvent]] from its `rawPayload`.
/// Every source‑specific field degrades gracefully to an empty default so the
/// frontend can render a single card template with no `if source = ...`
/// branching.
structure EnrichedEvent {
    // ── Core (mirrors IrishEvent, mutates into a flatten view) ──────────

    @required
    id: String

    @required
    title: String

    @required
    url: String

    @required
    startDate: String                 // YYYY-MM-DD

    startTime: String                 // HH:MM:SS (optional)

    endDate: String                   // YYYY-MM-DD — may fall back to startDate

    timezone: String                  // e.g. "Europe/London"

    status: String                    // e.g. "onsale", "cancelled", "postponed"

    @required
    city: String

    county: IrishCounty

    coordinates: GeoCoordinates

    @required
    source: String                    // e.g. "Ticketmaster", "Eventbrite"

    // ── Rich content (extracted from rawPayload, empty defaults) ────────

    images: EnrichedImagesList        // all available image variants

    venueName: String

    venueAddress: String

    genre: String                     // e.g. "Music / Metal"

    subGenre: String                  // e.g. "Nu-Metal"

    artistNames: StringsList

    artistLinks: ArtistLinks

    description: String               // event info / summary / pleaseNote

    salesStart: String                // public sale start ISO‑8601

    salesEnd: String                  // public sale end ISO‑8601
}

list StringsList {
    member: String
}

// ── Images ────────────────────────────────────────────────────────────────

list EnrichedImagesList {
    member: EnrichedImage
}

structure EnrichedImage {
    @required
    url: String

    ratio: String                     // e.g. "16_9", "3_2"

    width: Integer

    height: Integer
}

// ── Artist ────────────────────────────────────────────────────────────────

/// Social / external links for the primary attraction.
structure ArtistLinks {
    spotify: String
    youtube: String
    instagram: String
    twitter: String
    facebook: String
    website: String
    wikipedia: String
    tiktok: String
}

list EnrichedEventList {
    member: EnrichedEvent
}
