import java.io._
import scala.io._

object RoutesDataFileGenerator {

  def generate(gtfsPath: String, routesDataFile: String): Unit = {

    def tokenizeCSVLine(line: String): Array[String] = line.split(",")

    case class CsvRecord(tripId: String, rest: String)
    def stopTimeFromTokens(tokens: Array[String]) = CsvRecord(tokens(0), tokens.patch(0, Nil, 1).mkString(","))
    def tripFromTokens(tokens: Array[String]) = CsvRecord(tokens(2), tokens.patch(2, Nil, 1).mkString(","))

    val stopTimes: List[CsvRecord] = Source
      .fromFile(s"$gtfsPath/stop_times.txt")
      .getLines
      .map(tokenizeCSVLine)
      .map(stopTimeFromTokens)
      .toList

    val trips: List[CsvRecord] = Source
      .fromFile(s"$gtfsPath/trips.txt")
      .getLines
      .map(tokenizeCSVLine)
      .map(tripFromTokens)
      .toList
    val tripsMap = trips
      .map(csvRecord => csvRecord.tripId -> csvRecord.rest)
      .toMap

    def joinStopTimeWithTrip(stopTime: CsvRecord) = s"${stopTime.tripId},${stopTime.rest},${tripsMap(stopTime.tripId)}"
    val output = stopTimes.map(joinStopTimeWithTrip)

    val file = new File(routesDataFile)
    val pw = new PrintWriter(file)
    output.foreach { line => pw.write(s"$line\n") }
    pw.close()
  }
}