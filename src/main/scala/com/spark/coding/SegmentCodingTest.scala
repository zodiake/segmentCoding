package com.spark.coding

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by wangqi08 on 23/6/2016.
  */
object SegmentCodingTest {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    conf.setAppName("BrandCoding")
    val sc = new SparkContext(conf)
    val segmentConfigRDD = sc.textFile("D:/wangqi/testFile/SEGCONF").map(_.split(",")).cache

    val cateConf = sc.textFile("D:/wangqi/testFile/CATCONF").map(_.split(",")).map { case Array(a, b) => (a, b) }.collectAsMap
    val cateConfBroadCast = sc.broadcast(cateConf)
    def prepareCateCode(item: String): String = {
      val head = item.split(",")(0)
      val cateTrans = cateConfBroadCast.value.get(head.toUpperCase())
      if (cateTrans.isDefined) {
        s"${item.replace(head, cateTrans.get)},${head}"
      } else {
        s"${item}, "
      }
    }
    val sourceRDD = sc.textFile("D:/wangqi/testFile/part-00000")
      .map(prepareCateCode)
      .map(_.split(","))
      .map(i => ItemSegment(i(0), i(1), s"${i(2)} ${i(3)} ${i(4)}", s"${i(1)}".substring(0, 8), s"${i(1)}".substring(8, 13), i(8)))
      .map(i => (i.cateCode.toUpperCase(), i))
      .partitionBy(new HashPartitioner(100))

    val configRDD = segmentConfigRDD
      .filter(i => i(3) != "BRAND" && !i(3).contains("SUBBRAND") && i(3) != "PACKSIZE" && i(3) != "PRICETIER" && i(3) != "CATEGORY")
      .map(i => ((i(1), i(3)), i))
      .aggregateByKey(List[Array[String]]())((j, k) => k :: j, (u, v) => u ++ v)
      .map(i => (i._1._1, (i._1._2, i._2)))
      .groupByKey

    val joined = sourceRDD.join(configRDD)

    val kra = List(
      (1, "梳打;苏打"),
      (2, "威化;华夫;莱家;CCS;丽芝士;爱利地;奇巧;{威化卷};{wafer roll}"),
      (3, "夹心"),
      (4, "曲奇;馅饼;脆饼;椰圈;大润谷;卜珂;金罐;巴拿米;Danisa;丹麦皇冠;趣多多;{迷你};{Mini};丹麦蓝罐"),
      (5, "蛋卷"),
      (6, "甜;咸;薄饼;无糖;低糖;全麦;消化;消食;玛利;玛丽;韧性;麦香"),
      (7, "其他")
    )


    joined.map {
      case ((cateCode, (item, keywords))) => {
        keywords.map(i => {
          if (cateCode == "FB" && i._1 == "CAPACITY") {

          } else if (cateCode == "IMF" && i._1 == "STAGE") {


          } else if ((cateCode.contains("_BUNDLE") || cateCode.contains("BANDEDPACK_")) && i._1 == "SUBCATEGORY") {

          } else if (cateCode == "SP" && i._1 == "LENGTH") {

          } else if (cateCode == "BIS" && i._2 == "KRASEGMENT") {

          } else {

          }
        })
      }
    }
  }
}
