package com.spark

import com.nielsen.packsize.PacksizeCoding

object PacksizeTest {
  def main(arr: Array[String]): Unit = {
    def n(n: Int) = n
    lazy val one = n(1)
    lazy val two = n(2)
    val stream = Stream(one, two)
    println(stream)
  }
}