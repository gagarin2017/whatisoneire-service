$version: "2"

namespace what.is.on.eire

/// Representation of an active regional event in Ireland
structure IrishEvent {
    @required
    id: String,
    
    @required
    title: String,
    
    @required
    url: String,
    
    @required
    startDate: String, // Format: YYYY-MM-DD
    
    startTime: String, // Format: HH:MM:SS (Optional)
    
    @required
    city: String,
    
    @required
    county: IrishCounty,
    
    coordinates: GeoCoordinates,
    
    @required
    source: String
}

enum IrishCounty {
    DUBLIN = "IE-D"
    GALWAY = "IE-G"
    CORK = "IE-C"
    MEATH = "IE-MH"
    UNKNOWN = "UNKNOWN"
}

structure GeoCoordinates {
    @required
    latitude: Double,
    @required
    longitude: Double
}