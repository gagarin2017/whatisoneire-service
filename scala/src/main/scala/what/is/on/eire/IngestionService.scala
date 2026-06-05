package what.is.on.eire

/** Orchestrates the ingestion flow for a single data source.
  *
  * Takes two pure functions as constructor parameters:
  *   - `fetchByCity`: fetches events filtered by a specific city
  *   - `fetchAll`: fetches all events (no city filter, used by cron trigger)
  *
  * This design makes the service fully testable — pass simple `IO.pure(...)` functions in tests
  * instead of real API clients.
  *
  * When new data sources are added (Meetup, FailteIreland, etc.), compose their fetch functions
  * into the same interface.
  *
  * @tparam F
  *   the effect type (e.g. `IO`, `cats.Id` in tests)
  * @param fetchByCity
  *   fetches events for a given city name
  * @param fetchAll
  *   fetches all events (cron trigger, no filter)
  */
class IngestionService[F[_]](
  fetchByCity: String => F[List[IrishEvent]],
  fetchAll: F[List[IrishEvent]]
) {

  /** Parse the `irish-location` header from the raw Lambda input and dispatch to the right fetch
    * function.
    *
    * @param rawInput
    *   the full Lambda invocation payload as a JSON string
    * @return
    *   the list of events wrapped in the effect type `F`
    */
  def fetchEvents(rawInput: String): F[List[IrishEvent]] = {
    val requestedCity = CityHeaderParser.parse(rawInput)
    requestedCity match {
      case Some(city) => fetchByCity(city)
      case None       => fetchAll
    }
  }

}
