import java.util.Date
import java.text.SimpleDateFormat
import scala.io._

case class GtfsStop(stopId: String,
                    stopName: String,
                    stopLat: String,
                    stopLon: String,
                    locationType: String,
                    parentStation: String)

case class RouteData(tripId: String,
                     arrivalTime: String,
                     departureTime: String,
                     stopId: String,
                     stopSequence: String,
                     routeId: String,
                     serviceId: String,
                     directionId: String,
                     shapeId: String)

object DataLoader {
  private def tokenizeCSVLine(line: String): Array[String] = line.split(",").map(_.stripPrefix("\"").stripSuffix("\""))

  def loadStops(gtfsPath: String): List[GtfsStop] = {
    def gtfsStopFromTokens(tokens: Array[String]) = GtfsStop(stopId = tokens(0),
      stopName = tokens(1),
      stopLat = tokens(2),
      stopLon = tokens(3),
      locationType = tokens(4),
      parentStation = tokens(5))

    Source
      .fromFile(s"$gtfsPath/stops.txt")
      .getLines()
      .map(tokenizeCSVLine)
      .map(gtfsStopFromTokens)
      .toList
  }

  // WARNING: retrieving only today services doesn't work well
  // with bus having trips that span multiple dates
  def loadTodayRoutesData(gtfsPath: String, routesDataFile: String): List[RouteData] = {

    case class CalendarDate(serviceId: String, date: String, exceptionType: String)
    def calendarDateFromTokens(tokens: Array[String]) = CalendarDate(serviceId = tokens(0),
      date = tokens(1),
      exceptionType = tokens(2))

    val format = new SimpleDateFormat("yyyyMMdd")
    val today = format.format(new Date())
    val todayServicesIds = Source
      .fromFile(s"$gtfsPath/calendar_dates.txt")
      .getLines()
      .map(tokenizeCSVLine)
      .map(calendarDateFromTokens)
      .filter(_.date == today)
      .map(_.serviceId)
      .toList

    def routeDataFromTokens(tokens: Array[String]) = RouteData(tripId = tokens(0),
      arrivalTime = tokens(1),
      departureTime = tokens(2),
      stopId = tokens(3),
      stopSequence = tokens(4),
      routeId = tokens(5),
      serviceId = tokens(6),
      directionId = tokens(7),
      shapeId = tokens(8))

    Source
      .fromFile(routesDataFile)
      .getLines()
      .map(tokenizeCSVLine)
      // optimization to create less routeData objects
      // same as .filter(routeData => todayServicesIds.contains(routeData.serviceId))
      .filter(tokens => todayServicesIds.contains(tokens(6)))
      // optimization to create less routeData objects
      // same as .filterNot(_.arrivalTime.matches("^[2-9][4-9].*"))
      // WARNING: Trips that span multiple dates will have stop times greater than 24:00:00.
      // For example, if a trip begins at 10:30:00 p.m. and ends at 2:15:00 a.m. on the following day,
      // the stop times would be 22:30:00 and 26:15:00.
      // we ignore these trips
      .filterNot(_(1).matches("^[2-9][4-9].*"))
      .map(routeDataFromTokens)
      .toList
  }
}