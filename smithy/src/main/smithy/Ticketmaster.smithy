$version: "2"

namespace what.is.on.eire

use alloy#simpleRestJson

// ── External Ticketmaster API modelled as a Smithy4s service ──────────────

@simpleRestJson
service TicketmasterApi {
    version: "1.0.0"
    operations: [GetTicketmasterEvents]
}

@http(method: "GET", uri: "/discovery/v2/events.json")
operation GetTicketmasterEvents {
    input: GetTicketmasterEventsInput
    output: TicketmasterResponse
}

structure GetTicketmasterEventsInput {
    @required
    @httpQuery("countryCode")
    countryCode: String,

    @httpQuery("city")
    city: String,

    @required
    @httpQuery("apikey")
    apiKey: String,

    @httpQuery("size")
    size: Integer,

    @httpQuery("page")
    page: Integer,
}

// ── Response models ───────────────────────────────────────────────────────

structure TicketmasterResponse {
    @required
    _embedded: TicketmasterEmbeddedEvents,

    @required
    page: TicketmasterPage,
}

structure TicketmasterPage {
    @required
    size: Integer,

    @required
    totalElements: Integer,

    @required
    totalPages: Integer,

    @required
    number: Integer,
}

structure TicketmasterEmbeddedEvents {
    @required
    events: TicketmasterEventsList
}

list TicketmasterEventsList {
    member: TicketmasterEvent
}

// ── Event (top level) ─────────────────────────────────────────────────────

structure TicketmasterEvent {
    @required
    name: String

    @required
    id: String

    url: String
    locale: String

    images: TicketmasterImages
    sales: TicketmasterSales

    @required
    dates: TicketmasterDates

    classifications: TicketmasterClassifications

    promoter: TicketmasterPromoter
    promoters: TicketmasterPromoters

    info: String
    pleaseNote: String

    seatmap: TicketmasterSeatmap

    @required
    _embedded: TicketmasterEventEmbedded
}

// ── Images ────────────────────────────────────────────────────────────────

list TicketmasterImages {
    member: TicketmasterImage
}

structure TicketmasterImage {
    @required
    url: String
    ratio: String
    width: Integer
    height: Integer
    fallback: Boolean
}

// ── Sales ─────────────────────────────────────────────────────────────────

structure TicketmasterSales {
    @required
    public: TicketmasterSalePeriod
}

structure TicketmasterSalePeriod {
    startDateTime: String
    endDateTime: String
}

// ── Dates (extended) ──────────────────────────────────────────────────────

structure TicketmasterDates {
    @required
    start: TicketmasterDatePoint
    end: TicketmasterDatePoint
    timezone: String
    status: TicketmasterDateStatus
}

structure TicketmasterDatePoint {
    localDate: String
    localTime: String
}

structure TicketmasterDateStatus {
    code: String
}

// ── Classifications ───────────────────────────────────────────────────────

list TicketmasterClassifications {
    member: TicketmasterClassification
}

structure TicketmasterClassification {
    primary: Boolean
    segment: TicketmasterCategory
    genre: TicketmasterCategory
    subGenre: TicketmasterCategory
}

structure TicketmasterCategory {
    @required
    id: String
    @required
    name: String
}

// ── Promoter ──────────────────────────────────────────────────────────────

structure TicketmasterPromoter {
    id: String
    name: String
    description: String
}

list TicketmasterPromoters {
    member: TicketmasterPromoter
}

// ── Seatmap ───────────────────────────────────────────────────────────────

structure TicketmasterSeatmap {
    staticUrl: String
}

// ── Event-level _embedded (venues + attractions) ──────────────────────────

structure TicketmasterEventEmbedded {
    venues: TicketmasterVenuesList
    attractions: TicketmasterAttractionsList
}

// ── Venue ─────────────────────────────────────────────────────────────────

list TicketmasterVenuesList {
    member: TicketmasterVenue
}

structure TicketmasterVenue {
    name: String
    id: String
    url: String

    city: TicketmasterCity
    country: TicketmasterCountry
    address: TicketmasterAddress
    postalCode: String
    location: TicketmasterLocation
    timezone: String

    images: TicketmasterImages

    boxOfficeInfo: TicketmasterBoxOfficeInfo
    parkingDetail: String
    accessibleSeatingDetail: String

    generalInfo: TicketmasterGeneralInfo
}

structure TicketmasterCity {
    name: String
}

structure TicketmasterCountry {
    name: String
    countryCode: String
}

structure TicketmasterAddress {
    line1: String
}

structure TicketmasterLocation {
    latitude: String
    longitude: String
}

structure TicketmasterBoxOfficeInfo {
    phoneNumberDetail: String
    openHoursDetail: String
    acceptedPaymentDetail: String
}

structure TicketmasterGeneralInfo {
    childRule: String
}

// ── Attraction / Artist ───────────────────────────────────────────────────

list TicketmasterAttractionsList {
    member: TicketmasterAttraction
}

structure TicketmasterAttraction {
    @required
    name: String
    @required
    id: String

    url: String

    images: TicketmasterImages
    classifications: TicketmasterClassifications
    externalLinks: TicketmasterExternalLinks
}

structure TicketmasterExternalLinks {
    youtube: TicketmasterLinkList
    twitter: TicketmasterLinkList
    itunes: TicketmasterLinkList
    lastfm: TicketmasterLinkList
    tiktok: TicketmasterLinkList
    spotify: TicketmasterLinkList
    wiki: TicketmasterLinkList
    facebook: TicketmasterLinkList
    instagram: TicketmasterLinkList
    homepage: TicketmasterLinkList
    musicbrainz: TicketmasterMusicbrainzList
}

list TicketmasterLinkList {
    member: TicketmasterLink
}

structure TicketmasterLink {
    url: String
}

list TicketmasterMusicbrainzList {
    member: TicketmasterMusicbrainz
}

structure TicketmasterMusicbrainz {
    id: String
    url: String
}
