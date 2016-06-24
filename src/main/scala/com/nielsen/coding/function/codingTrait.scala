/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.nielsen.coding.function

trait CodingBaseTrait {
  def StrSearch: Int
}


trait StrTrait {
  def StrSearch(itemDesc: String, targetDesc: String): Int = {
    val tarlist = targetDesc.split(";")
    val templist = tarlist.map(x => findDesc(itemDesc, x))
    val result = if (templist.max >= 0) templist.max else -1
    if (targetDesc.contains("{") && targetDesc.contains("}")) {
      val l = targetDesc.indexOf("{")
      val r = targetDesc.indexOf("}")
      if (l < r) {
        val excludedesc = targetDesc.substring(l + 1, r)
        if (findDesc(itemDesc, excludedesc) >= 0) {
          -1
        } else result
      } else result
    } else result
  }

  def findDesc(itemDesc: String, targetDesc: String): Int = {
    if (targetDesc.indexOf("/") >= 0) { //如果存在斜杠分割的字符串则逐个寻找
      val targetlist = targetDesc.split("/").toList.map(x => itemDesc.indexOf(x))
      if (targetlist.min >= 0) { //如果每个分割的字符串都能找到则返回最前面的位置
        targetlist.min
      } else
        -1
    } else
      itemDesc.indexOf(targetDesc) //没有斜杠的字符串按正常方式查找
  }
}


trait StrTraitB extends StrTrait {
  def StrSearchB(itemDesc: String, targetDesc: String): Boolean = {
    if (super.StrSearch(itemDesc, targetDesc) >= 0) {
      true
    } else false
  }
}


trait OrderTrait extends StrTraitB {
  def PrimOrderAsc(itemDesc: String, targetArr: Seq[(Int, String)]):Seq[(Int, String)] = {
    val matchNum = targetArr.minBy(_._1)._1  //排在数组前面的优先
    val matchArr = targetArr.filter(_._1 == matchNum)
    return matchArr
  }
  
  def PosiOrderDesc(itemDesc: String, targetArr: Seq[(Int, String)]):(Int, String) = {
    val result = targetArr.map(x => (x._1, x._2, StrSearch(itemDesc, x._2)))
      .maxBy(_._3)  //关键词在后面的优先
    return (result._1, result._2)
  }
}
