package what.is.on.eire

class EventProcessor {
  def process(event: IrishEvent): String = {
    // All your actual business logic, database calls, or calculations happen here
    s"Successfully processed ${event.title} in County ${event.county}"
  }
}
