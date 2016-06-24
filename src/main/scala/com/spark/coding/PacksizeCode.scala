package com.spark.coding

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.spark.model.Item

object PackSizeCode {
  def main(arg: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    conf.setAppName("TotalCoding")
    val sc = new SparkContext(conf)
    val segmentConfigRDD = sc.textFile("D:/wangqi/testFile/SEGCONF").map(_.split(",")).cache
    val catList = segmentConfigRDD.map(_(1)).distinct.collect()
    val pack = segmentConfigRDD.filter(_(3).toUpperCase() == "PACKSIZE").map(i => (i(1), i(5).split(";"))).filter(i => i._2.size > 0)

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

    val sourceRDD = sc.textFile("D:/wangqi/testFile/part-00001")
      .map(prepareCateCode)
      .map(_.split(","))
      .map(i => Item(i(0), i(1), s"${i(2)} ${i(3)} ${i(4)},${i(5)}".toUpperCase, s"${i(1)}".substring(0, 8), s"${i(1)}".substring(8, 13)))
      .filter(i => catList.contains(i.cateCode.toUpperCase()))
      .map(i => (i.cateCode.toUpperCase(), i))
      .partitionBy(new HashPartitioner(100))

    case class Pack(code: String, pack: String, name: String)
    val packsizeConfig = segmentConfigRDD.filter(_(3).toUpperCase() == "PACKSIZE").map(i => (i(1), Pack(i(2), i(5), i(10))))
    val joined = sourceRDD.join(packsizeConfig)

    def replaceC2E(CS: String): String = {
      if (CS == "克") {
        "G"
      } else if (CS == "千克") {
        "KG"
      } else if (CS == "毫升") {
        "ML"
      } else if (CS == "升") {
        "L"
      } else if (CS == "公升") {
        "L"
      } else if (CS == "公斤") {
        "KG"
      } else if (CS == "盎司") {
        "OZ"
      } else if (CS == "片") {
        "P"
      } else if (CS == "斤") {
        "J"
      } else CS
    }

    val p = joined.map {
      case (category, (item, pack)) => {
        import com.nielsen.packsize.PacksizeCoding
        val description = item.brandDescription
        val b = pack.pack.split(";").map(replaceC2E).map(i => {
          (i, PacksizeCoding.getPacksize(description, i))
        }).maxBy {
          case (i, Some(d)) => d
          case (i, None)    => -1
        }
        if (b._2 == Some(0))
          (item, pack.code, (b._1, None))
        else
          (item, pack.code, b)
      }
    }

    val result = p.map {
      case (item, code, (i, Some(e))) => {
        s"${item.id},${code},${e}${i},${e}${i},${item.perCode},${item.storeCode}"
      }
      case (item, code, (i, None)) => {
        if (item.cateCode == "IMF") {
          s"${item.id},${code},0G,0G,${item.perCode},${item.storeCode}"
        } else if (item.cateCode == "DIAP") {
          s"${item.id},${code},0P,0P,${item.perCode},${item.storeCode}"
        } else {
          s"${item.id},${code},UNKNOWN,UNKNOWN,${item.perCode},${item.storeCode}"
        }
      }
    }
    result.take(200).foreach(println)
  }
}