package com.spark.coding

import com.nielsen.model.{IdAndKeyWordAndParentNo, Par, SegIdWithIndexAndSegName}
import com.spark.model.Item
import org.apache.spark.{SparkConf, SparkContext}

object SubBrandCoding {
  def main(args: Array[String]): Unit = {
    val separator = sys.props("line.separator")
    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    conf.setAppName("TotalCoding")
    val sc = new SparkContext(conf)
    val (subBrand, categoryConfig) = SegmentUtils.getConfig("SUBBRAND")

    val categoryCacheList = sc.broadcast(categoryConfig.map {
      i => (i(1).toUpperCase(), i(0))
    }.toMap).value

    val itemCacheList = sc.broadcast(subBrand.map { i =>
      (i(0), i(10))
    }.toMap).value

    val cateConfBroadCast = sc.broadcast(SegmentUtils.getConfigFile("/CATCONF.txt").getLines().map(_.split(",")).map {
      case Array(a, b) => (a, b)
    }.toMap).value

    val sourceRDD = sc.textFile(args(0))
      .map(SegmentUtils.prepareItem(_, cateConfBroadCast))
      .map(_.split(","))
      .filter(_.length > 2)
      .map(i => Item(i(0), i(1), s"${i(2)} ${i(3)} ${i(4)}", s"${i(1)}".substring(0, 8), s"${i(1)}".substring(8, 13)))
      .map(i => (i.cateCode.toUpperCase(), i))

    val c = sc.broadcast(subBrand.groupBy(_ (1)).map {
      case (key, value) =>
        val set = value.map(i => IdAndKeyWordAndParentNo(i(0), i(5), i.last))
        (key, set)
    }).value

    val result = sourceRDD.filter(i => c.keys.toSet.contains(i._2.cateCode)).map {
      case (cateCode, item) =>
        val brandDesc = item.brandDescription
        def extractBrand(list: List[IdAndKeyWordAndParentNo]): List[SegIdWithIndexAndSegName] = {
          implicit val alwaysTrue = (i: String) => true
          list.flatMap(Par.parse(_)(brandDesc))
        }
        val b = SegmentUtils.filterParentId(brandDesc, extractBrand(c.get(item.cateCode).get))
        val categoryString = s"${item.id},20,${categoryCacheList(item.cateCode)},${item.cateCode},${item.perCode},${item.storeCode}"

        b.sortBy(i => (i.index, i.par.length())) match {
          case Nil =>
            val string = s"${item.id},3000,UNKNOWN,UNKNOWN,${item.perCode},${item.storeCode}"
            s"${string}${separator}${categoryString}"
          case h :: t =>
            val t = itemCacheList.get(h.segmentId).get
            val string = s"${item.id},3000,${h.segmentId},${t},${item.perCode},${item.storeCode}"
            s"${string}${separator}${categoryString}"
        }
    }

    //result.saveAsTextFile(args(1))
    result.take(10).foreach(println)
  }
}
