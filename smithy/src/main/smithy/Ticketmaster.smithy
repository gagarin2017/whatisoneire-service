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

    @required
    @httpQuery("city")
    city: String,

    @required
    @httpQuery("apikey")
    apiKey: String,
}

// ── Response models ───────────────────────────────────────────────────────

structure TicketmasterResponse {
    @required
    _embedded: TicketmasterEmbeddedEvents
}

structure TicketmasterEmbeddedEvents {
    @required
    events: TicketmasterEventsList
}

list TicketmasterEventsList {
    member: TicketmasterEvent
}

structure TicketmasterEvent {
    @required
    id: String,

    @required
    name: String,

    @required
    url: String,

    @required
    dates: TicketmasterDates,

    @required
    _embedded: TicketmasterEmbeddedVenues
}

structure TicketmasterDates {
    @required
    start: TicketmasterStart
}

structure TicketmasterStart {
    @required
    localDate: String,
    localTime: String
}

structure TicketmasterEmbeddedVenues {
    @required
    venues: TicketmasterVenuesList
}

list TicketmasterVenuesList {
    member: TicketmasterVenue
}

structure TicketmasterVenue {
    name: String,
    city: TicketmasterCity,
    location: TicketmasterLocation
}

structure TicketmasterCity {
    name: String
}

structure TicketmasterLocation {
    latitude: String,
    longitude: String
}
