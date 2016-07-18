package com.spark.coding

import com.nielsen.model.SegIdWithIndexAndSegName

/**
  * Created by wangqi08 on 11/7/2016.
  */
object SegmentUtils {
  val segConfigPath = "/SEGCONF.txt"

  def getConfig(segmentName: String) = {
    val segConfig = getConfigFile(segConfigPath).getLines().map(_.split(",")).toList
    val config = segConfig.filter(_ (3).toUpperCase == segmentName)
    val categoryConfig = segConfig.filter(_ (3).toUpperCase == "CATEGORY")
    (config, categoryConfig)
  }

  def prepareItem(item: String, cateConfBroadCast: Map[String, String]): String = {
    val head = item.split(",")(0)
    val cateTrans = cateConfBroadCast.get(head.toUpperCase())
    cateTrans match {
      case None => s"${item},"
      case Some(e) => s"${item.replace(head, cateTrans.get)},${head}"
    }
  }

  def getConfigFile(path: String) = scala.io.Source.fromInputStream(getClass.getResourceAsStream(path))

  def filterParentId(brandDesc: String, list: List[SegIdWithIndexAndSegName]): List[SegIdWithIndexAndSegName] = {
    val parentId = list.map(i => i.parentNo).toSet.filter { i => i == "-" }

    for {
      j <- list if !parentId.contains(j.segmentId)
      k <- list.filter(_ != j) if (k.par.indexOf(j.par) > -1 && k.par.length > j.par.length)
    } yield
      k
  }

  def replaceC2E(s: String): String = {
    if (s == "克") {
      "G"
    } else if (s == "千克") {
      "KG"
    } else if (s == "毫升") {
      "ML"
    } else if (s == "升") {
      "L"
    } else if (s == "公升") {
      "L"
    } else if (s == "公斤") {
      "KG"
    } else if (s == "盎司") {
      "OZ"
    } else if (s == "片") {
      "P"
    } else if (s == "斤") {
      "J"
    } else s
  }
}
