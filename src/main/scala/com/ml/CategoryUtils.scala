package com.ml

/**
  * Created by wangqi08 on 1/7/2016.
  */
object CategoryUtils {
  val list = List("180", "14", "168", "130", "120", "149", "37", "45", "46", "131", "170", "163", "59", "89", "44", "148", "136", "140", "184", "39", "11", "133", "143", "65", "132", "23", "422", "155", "47", "67", "186", "5", "106", "75", "142", "32", "85", "54", "72", "71", "58", "111", "152", "181", "12", "99", "70", "177", "95", "125", "126", "129")

  val skinVector = Vector("281", "701703814", "702730", "704733", "4", "704729733", "282", "704729895", "701702", "701703729", "283", "312", "127", "3429195", "2", "703982", "9", "3", "729895", "93", "704981", "258", "89", "30", "270", "703704729", "342933", "703704793", "32933195", "703856", "44", "703895", "703733", "729958", "701704", "1", "703704", "733971", "702733", "704895", "29", "422", "1291930", "703971", "704733895", "703704733", "101", "703704895", "106", "729733", "704793", "703729733", "704729", "702703", "703793", "71", "50", "195", "156", "33", "167", "703729895", "128", "703729", "12", "38", "271", "102", "701703", "34156195", "114")

  def categoryToInt(s: String): Int = {
    val map = list.zipWithIndex.toMap
    map(s)
  }

  def skinCategoryToInt(s: String): Int = {
    val map = skinVector.zipWithIndex.toMap
    map(s)
  }
}
