package com.spark

import com.nielsen.model.IdAndKeyWord

object Coding {
  def main(args: Array[String]): Unit = {

    val segConfigBroadcast = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/SEGCONF.txt")).getLines().map(_.split(",")).filter(_ (3).toUpperCase == "BRAND").toList

    val c = segConfigBroadcast.groupBy(_ (1)).map {
      case (key, value) => {
        val set = value.map(i => IdAndKeyWord(i(0), i(5), i.last))
        (key, set)
      }
    }
    println(c.size)
  }
}