package com.spark

object Main {
  /*
    transaction-1: a,b,c
		transaction-2: a,b,d
	  transaction-3: b,c
	  transaction-4: b,c
   */
  def main(string: Array[String]): Unit = {
    val a ="""\d*""".r
    val c = "【长清人家】500g柠檬蜂蜜纯手工自制 蜂蜜柠檬茶冲饮 包邮 500g*12"
    val pack = "g"
    val tran = a.findAllIn(c).matchData.map(_.matched).filter(size => {
      c.indexOf(size + pack) > -1
    }).toList.sortBy(_.toInt)
    tran.foreach(size => {
      val op = c.substring(c.indexOf(size + pack) + 1)
      if (op == "*" || op == "+") {
        size + tran.indexOf(tran.indexOf(size) + 1)
      }
    })
  }
}