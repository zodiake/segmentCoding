package com.ml

/**
  * Created by wangqi08 on 1/7/2016.
  */
object CategoryUtils {
  val list = List("180", "14", "168", "130", "120", "149", "37", "45", "46", "131", "170", "163", "59", "89", "44", "148", "136", "140", "184", "39", "11", "133", "143", "65", "132", "23", "422", "155", "47", "67", "186", "5", "106", "75", "142", "32", "85", "54", "72", "71", "58", "111", "152", "181", "12", "99", "70", "177", "95", "125", "126", "129")

  val skinVector = Vector("3433193", "701703814", "702730", "281", "4", "704729733", "282", "704733", "704729895", "701702", "701703729", "12330", "36", "283", "312", "703729971", "342933195", "127", "3429195", "2", "703982", "9", "701702703", "3", "93", "704981", "729895", "258", "89", "30", "270", "703704729", "342933", "703704793", "32933195", "703856", "44", "703895", "703733", "729958", "701704", "1", "703704", "733971", "702733", "704895", "29", "422", "1291930", "703971", "704733895", "703704733", "101", "733895", "703704895", "106", "729733", "704793", "703729733", "704729", "85", "702703", "703793", "71", "50", "195", "156", "33", "167", "73", "703729895", "128", "703729", "12", "704733793", "38", "271", "102", "701703", "34156195", "3429156", "114")

  val skinInt2WordMap = Map(281 -> "ACNEP",
    178 -> "ATD",
    18 -> "BABYP",
    38 -> "BABYW",
    12 -> "BANDEDPACK",
    270 -> "BATHSALT",
    195 -> "BBCREAM",
    14 -> "BIS",
    257 -> "BODYTLPW",
    46 -> "CALCIUM",
    251 -> "CDRYFRUIT",
    71 -> "CERE",
    156 -> "CF",
    107 -> "CONDOM",
    272 -> "COTTONPADS",
    85 -> "DCONF",
    114 -> "DEODORANT",
    715750 -> "DWL/LAUND",
    44 -> "EDOIL",
    282 -> "EXFOLIATINGP",
    271 -> "EYEMASK",
    4 -> "FACIA",
    704982 -> "FACIA/ACNEP",
    704733 -> "FACIA/FACIALMASK",
    704729 -> "FACIA/TONER",
    33 -> "FACIALMASK",
    733981 -> "FACIALMASK/ACNEP",
    30 -> "HAIR",
    106 -> "HERBALTEA",
    152 -> "HONEY",
    283 -> "HRP",
    101 -> "INSB",
    57 -> "INSE",
    173 -> "JUICE",
    50 -> "LAUND",
    167 -> "LGELS",
    93 -> "LIPPALM",
    5 -> "MILK",
    422 -> "NC",
    128 -> "PDF",
    127 -> "PHS",
    149 -> "PMC",
    1 -> "PWASH",
    701702704 -> "PWASH/SHAM/FACIA",
    701702730 -> "PWASH/SHAM/HAIR",
    701703 -> "PWASH/SKIN",
    701703729 -> "PWASH/SKIN/TONER",
    701729 -> "PWASH/TONER",
    7 -> "RAZOR",
    2 -> "SHAM",
    702730 -> "SHAM/HAIR",
    3 -> "SKIN",
    703704 -> "SKIN/FACIA",
    703704733 -> "SKIN/FACIA/FACIALMASK",
    703704729 -> "SKIN/FACIA/TONER",
    3429195 -> "SKIN/FACIA/TONER/BBCREAM",
    342933 -> "SKIN/FACIA/TONER/FACIALMASK",
    703733 -> "SKIN/FACIALMASK",
    703729 -> "SKIN/TONER",
    703729733 -> "SKIN/TONER/FACIALMASK",
    51 -> "SP",
    89 -> "TEA",
    29 -> "TONER",
    729733 -> "TONER/FACIALMASK",
    9 -> "TP",
    45 -> "VITAMINS"
  )

  val skinWord2IntMap = skinInt2WordMap.map(i => (i._2, i._1)).toMap

  def categoryToInt(s: String): Int = {
    val map = list.zipWithIndex.toMap
    if (s.length == 2) {
      map(s)
    } else {
      map("29")
    }
  }

  def skinCategoryToInt(s: String): Int = {
    val map = skinVector.zipWithIndex.toMap
    if (s.length == 2) {
      map(s)
    } else {
      map("29")
    }
  }
}
