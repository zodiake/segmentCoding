package com.other

/**
  * Created by wangqi08 on 6/7/2016.
  */
object Main {
  def transform(source: List[List[Int]], result: List[List[Int]] = List[List[Int]]()): List[List[Int]] = {
    source match {
      case Nil => result
      case h :: t => {
        h match {
          case Nil => result
          case h1 :: t1 => {
            val head = for {
              row <- source
            } yield {
              row.head
            }
            val tail = for {
              row <- source
            } yield {
              row.tail
            }
            transform(tail, head :: result)
          }
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/testData.csv")).getLines()
    val c = source.map(_.split(",").map(_.toInt).toList).toList
    val transformed = transform(c)
  }
}
