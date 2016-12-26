import java.io._
import java.util.Date

import com.datastax.driver.core.{Cluster, Session}

object BusesLatLonGenerator {

  def generate(cassandraIp: String, stops: List[GtfsStop], todayRoutesData: List[RouteData], time: Date): Unit = {
    // We need to normalize day/month/year/timezone infos
    // to make current time comparable with retrieved times from GTFS
    val normTimeMs = gtfsTimeFormat.parse(gtfsTimeFormat.format(time)).getTime

    todayRoutesData
      .groupBy( routeData => (routeData.routeId, routeData.tripId) )
      .par
      .foreach { case (_, todayOneBusData) =>
        lastAndNextPositions(normTimeMs, todayOneBusData, stops) match {
          case (Some(busInfo), Some(lastPosition), Some(nextPosition)) =>
            val currentPosition = calculateBusPosition(normTimeMs, lastPosition, nextPosition)
            println(s"{ trip_id: '${busInfo.tripId}', route_id: '${busInfo.routeId}', latitude: ${currentPosition.latitude}, longitude: ${currentPosition.longitude}, timestamp: '${time.getTime}' }")
          case _ =>
        }
      }
  }

  // We use a new instance of SimpleDateFormat each time we need it
  // Beacuse with only one shared instance, parallel access to it throws exceptions
  private def gtfsTimeFormat = new java.text.SimpleDateFormat("HH:mm:ss")

  private case class BusInfo(routeId: String, tripId: String, nextStop: Option[RouteData])
  private case class BusPosition(latitude: Double, longitude: Double, timeMs: Long)

  private def routeDataToBusPosition(routeData: RouteData, stops: List[GtfsStop]): BusPosition = {
    val stop = stops.filter(_.stopId == routeData.stopId).head
    val arrTimeMs = gtfsTimeFormat.parse(routeData.arrivalTime).getTime
    BusPosition(stop.stopLat.toDouble, stop.stopLon.toDouble, arrTimeMs)
  }

  private def lastAndNextPositions(timeMs: Long, todayOneBusData: List[RouteData], stops: List[GtfsStop]): (Option[BusInfo], Option[BusPosition], Option[BusPosition]) = {
    def getArrTimeMs(routeData: RouteData): Long = gtfsTimeFormat.parse(routeData.arrivalTime).getTime

    var busInfoOpt: Option[BusInfo] = None
    var lastPositionOpt: Option[BusPosition] = None
    var nextPositionOpt: Option[BusPosition] = None

    val (prevStops, nextStops) = todayOneBusData
      .partition( routeData => (timeMs - getArrTimeMs(routeData)) >= 0 )

    if(prevStops.nonEmpty) {
      val lastStop = prevStops
        .minBy( routeData => timeMs - getArrTimeMs(routeData) )

      lastPositionOpt = Some(routeDataToBusPosition(lastStop, stops))
      busInfoOpt = Some(BusInfo(lastStop.routeId, lastStop.tripId, None))
    }

    if(nextStops.nonEmpty) {
      val nextStop = nextStops
        .minBy( routeData => getArrTimeMs(routeData) - timeMs )

      nextPositionOpt = Some(routeDataToBusPosition(nextStop, stops))
      busInfoOpt = Some(BusInfo(nextStop.routeId, nextStop.tripId, Some(nextStop)))
    }

    (busInfoOpt, lastPositionOpt, nextPositionOpt)
  }

  private def calculateBusPosition(timeMs: Long, startPosition: BusPosition, endPosition: BusPosition): BusPosition = {
    /*
    Line equation given two points
    x = currentTime
    y = currentLat
    p1 (lastTime, lastLat)
    p2 (nextTime, nextLat)
             y2 - y1
        y = --------- * (x - x1) + y1
             x2 - x1
    */
    val latitude = ((endPosition.latitude - startPosition.latitude)/(endPosition.timeMs - startPosition.timeMs))*(timeMs - startPosition.timeMs) + startPosition.latitude
    val longitude = ((endPosition.longitude - startPosition.longitude)/(endPosition.timeMs - startPosition.timeMs))*(timeMs - startPosition.timeMs) + startPosition.longitude
    BusPosition(latitude, longitude, timeMs)
  }
}