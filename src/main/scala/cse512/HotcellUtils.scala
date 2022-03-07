package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(", "").toDouble / coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")", "").toDouble / coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser(timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART

  // A Score base on Getis-Ord statistic
  // http://sigspatial2016.sigspatial.org/giscup2016/problem
  def ZScore(x:Int, y:Int, z:Int, numCells: Int, hotCellAverage: Double, StandardDeviation: Double, NumOfadjacentCell: Int, SumNeighborhCell: Int ): Double = {
    var answer: Double = 0
    answer = (SumNeighborhCell.toDouble - (hotCellAverage * NumOfadjacentCell.toDouble)) /
      (StandardDeviation * math.sqrt(((NumOfadjacentCell.toDouble * numCells.toDouble) -
        (NumOfadjacentCell.toDouble * NumOfadjacentCell.toDouble)) / (numCells.toDouble - 1.0)))
    return answer
  }

  // Function : check for neighborhood cell. If the coordinate difference is more than 1 than return false.
  def neighborhoodCell(x1: Int, x2: Int, y1: Int, y2: Int, z1: Int, z2: Int): Boolean = {
    if (math.abs(x1 - x2) > 1 || math.abs(y1 - y2) > 1 || math.abs(z1 - z2) > 1)
    {return false}
    else
    {return true}
  }

  // Calculate number of neighbor each cell
  def NumOfadjacentCell(minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, x: Int, y: Int, z: Int): Long = {
    var adjacentcellcount = 0;
    if (x == minX || x == maxX) { adjacentcellcount += 1;}
    if (y == minY || y == maxY) {adjacentcellcount += 1;}
    if (z == minZ || z == maxZ) {adjacentcellcount += 1;}
    if (adjacentcellcount == 1) {return 18;}
    else if (adjacentcellcount == 2) {return 12;}
    else if (adjacentcellcount == 3) {return 8;}
    else {return 27;}
  }


}
