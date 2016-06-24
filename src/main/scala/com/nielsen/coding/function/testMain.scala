/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.nielsen.coding.function
import scala.io.Source

object testMain {

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {

    val a = "Ideans依蒂安斯曲奇巧克力布朗尼巧克力曲奇饼干依蒂安斯纯手工盒装低糖零食进口原料食品"

    //val item = "sgdsgsesvabcd123r"
    //val c = l.StrSearchB(item, a)
    //val d = abc.findDesc(item, "123/ge")
    val file = Source.fromFile("segfile_1").getLines.toArray
      .map(_.split(",")).map(x => (x(0).toInt, x(1).toInt, x(2)))
    val z = file.map(x => (x._2, x._3))
    val l = new zis(a, z)
    val y = l.kraseg
    val re = file.filter(x => x._3 == y._2).head
    println(re._1, re._2, re._3)
  }

  class zis(itemDesc: String, targetArr: Array[(Int, String)]) extends OrderTrait {
    def kraseg(): (Int, String) = {
      val other = targetArr.filter(_._2 == "其他")
      if(other.isEmpty){
        System.err.println("Please add the segment of 'OTHER'!")
        System.exit(1)
      }
      val strresult = targetArr.map(x => (x._1, x._2, StrSearchB(itemDesc, x._2)))
        .filter(_._3 == true)
        .map(x => (x._1, x._2))
      if (strresult.isEmpty) {
        other.head
      } else {
        val primarr = PrimOrderAsc(itemDesc, strresult)
        if (primarr.size > 1) {
          PosiOrderDesc(itemDesc, strresult)
        } else if (primarr.size == 1) {
          primarr.head
        } else other.head
      }
    }

  }
}
