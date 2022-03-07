package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val points = pointString.split(",")
    val pointx = points(0).trim.toDouble
    val pointy = points(1).trim.toDouble

    // Get reactangle coordinates input separate by ","
    val rectangle = queryRectangle.split(",")
    val bottom_left_x1 = rectangle(0).trim.toDouble
    val bottom_left_y1 = rectangle(1).trim.toDouble
    val top_right_x2 = rectangle(2).trim.toDouble
    val top_right_y2 = rectangle(3).trim.toDouble

    // Check if point x and y within rectangle boundary. If yes the return true
    if (pointx >= bottom_left_x1 && pointy >= bottom_left_y1 && pointx <= top_right_x2 && pointy <= top_right_y2) {
      return true
    } else {
      return false
    }
  }
}
