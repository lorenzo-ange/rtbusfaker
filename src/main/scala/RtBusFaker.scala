import java.io._
import java.util.Date

object RtBusFaker {

  private def loadData(gtfsPath: String, routesDataFile: String): (List[GtfsStop], List[RouteData]) = {

    val file = new File(routesDataFile)
    if(!file.exists) {
      println("Routes data file not present, generating it...")
      Utils.benchmark("Routes data file generation") { () =>
        RoutesDataFileGenerator.generate(gtfsPath, routesDataFile)
      }
    }

    println("Loading data...")
    val stops = Utils.benchmark("Stops loading") { () =>
      DataLoader.loadStops(gtfsPath)
    }
    val todayRoutesData = Utils.benchmark("Today routes data loading") { () =>
      DataLoader.loadTodayRoutesData(gtfsPath, routesDataFile)
    }
    (stops, todayRoutesData)
  }

  def main(args: Array[String]): Unit = {

    if(args.length != 2) {
      println("Usage: RtBusFaker [gtfsPath]")
      return
    }

    val gtfsPath = args(0)
    val routesDataFile = "routesData.csv"
    println("RtBusFaker started")
    println(s"gtfsPath : $gtfsPath")
    println(s"routesDataFile : $routesDataFile")

    val (stops, todayRoutesData) = loadData(gtfsPath, routesDataFile)
    if(todayRoutesData.isEmpty) {
      println("No data for today, are you using old gtfs data?")
      println("when updating gtfs data remember also to delete the routesData.csv file.")
      return
    }

    println("Starting output generation...")
    while(true) {
      Utils.benchmark("RtBusFaker output generation") { () =>
        BusesLatLonGenerator.generate(cassandraIp, stops, todayRoutesData, new Date())
      }
      Thread.sleep(10000L)
    }
  }
}
