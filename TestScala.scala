package com.pari.poc

object TestScala {


  def main(args: Array[String]): Unit = {
    println("Hello Welcome to Scala!")
    val result = encode("13.316046490521972,76.04377310835761", 6)
    println(result)
  }

  def encode(latlng: String, precision: Int): String = {
    val temp = latlng.split(",")
    val lat = temp(0).toDouble
    val lng = temp(1).toDouble
    println(lat)
    println(lng)

    val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    var (minLat, maxLat) = (-90.0, 90.0)
    var (minLng, maxLng) = (-180.0, 180.0)
    val bits = List(16, 8, 4, 2, 1)

    (0 until precision).map { p => {
      base32 apply (0 until 5).map { i => {
        if (((5 * p) + i) % 2 == 0) {
          val mid = (minLng + maxLng) / 2.0
          if (lng > mid) {
            minLng = mid
            bits(i)
          } else {
            maxLng = mid
            0
          }
        } else {
          val mid = (minLat + maxLat) / 2.0
          if (lat > mid) {
            minLat = mid
            bits(i)
          } else {
            maxLat = mid
            0
          }
        }
      }
      }.reduceLeft((a, b) => a | b)
    }
    }.mkString("")
  }
}
