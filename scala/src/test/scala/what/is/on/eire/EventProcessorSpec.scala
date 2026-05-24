package what.is.on.eire

import org.scalatest.funsuite.AnyFunSuite
import what.is.on.eire.IrishEvent.id

class EventProcessorSpec extends AnyFunSuite {

  test("process returns a success message containing event title and county") {
    val processor = new EventProcessor

    val event = IrishEvent(
      id = "123",
      title = "Galway Food Festival",
      url = "http://www.ticketmaster.ie/event/123",
      startDate = "24/05/2026",
      city = "Dublin",
      county = IrishCounty.DUBLIN,
      source = "Ticketmaster",
      startTime = None,
      coordinates = None
    )

    val result = processor.process(event)

    assert(
      result == "Successfully processed Galway Food Festival in County DUBLIN"
    )
  }
}
