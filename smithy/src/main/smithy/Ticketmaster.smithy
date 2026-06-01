$version: "2"

namespace what.is.on.eire

// Representing the top-level Ticketmaster API Response
structure TicketmasterResponse {
    _embedded: TicketmasterEmbeddedEvents
}

// Unpacks the embedded block containing the events list
structure TicketmasterEmbeddedEvents {
    events: TicketmasterEventsList
}

// A list of raw Ticketmaster events
list TicketmasterEventsList {
    member: TicketmasterEvent
}

// Represents a single Ticketmaster event in the response array
structure TicketmasterEvent {
    @required
    id: String,

    @required
    name: String, // Maps to IrishEvent 'title'

    @required
    url: String,

    dates: TicketmasterDates,

    _embedded: TicketmasterEmbeddedVenues
}

// Unpacks the dates object
structure TicketmasterDates {
    start: TicketmasterStart
}

// Unpacks the start date and optional time
structure TicketmasterStart {
    @required
    localDate: String, // Maps to IrishEvent 'startDate'
    localTime: String  // Maps to IrishEvent 'startTime'
}

// Unpacks the embedded venues array inside the event
structure TicketmasterEmbeddedVenues {
    venues: TicketmasterVenuesList
}

// A list of venues where the event takes place
list TicketmasterVenuesList {
    member: TicketmasterVenue
}

// Represents the venue details
structure TicketmasterVenue {
    name: String,
    city: TicketmasterCity,
    location: TicketmasterLocation
}

// Unpacks the city details
structure TicketmasterCity {
    name: String
}

// Unpacks coordinates
structure TicketmasterLocation {
    latitude: String,
    longitude: String
}
