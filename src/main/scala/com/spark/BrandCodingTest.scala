package com.spark

import com.nielsen.model.BothPar

object BrandCodingTest {
  def main(args: Array[String]): Unit = {
    println(BothPar("MM豆", "mm").parse("申通包邮 法国 MM豆MMS巧克力豆机M&amp;M'S 圣诞老人糖果机 全新款式"))
  }
}