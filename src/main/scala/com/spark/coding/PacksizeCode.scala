package com.spark.coding

import com.spark.model.Item
import org.apache.spark.{SparkConf, SparkContext}

case class Pack(id: String, pack: String, name: String)

object PackSizeCode {

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

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    val conf = new SparkConf().setMaster("local[2]").setAppName("My App")
    conf.setAppName("PackCoding")
    val sc = new SparkContext(conf)

    val segConfig = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/SEGCONF.txt"))
      .getLines().map(_.split(",")).toList
    val packConfig = segConfig.filter(_ (3).toUpperCase == "PACKSIZE")
    val categoryConfig = segConfig.filter(_ (3).toUpperCase == "CATEGORY")
    val separator = sys.props("line.separator")

    val packBroadcast = sc.broadcast(packConfig.map {
      i => (i(1), Pack(i(2), i(5), i(10)))
    }.toMap).value

    val categoryCacheList = sc.broadcast(categoryConfig.map {
      i => (i(1).toUpperCase(), i(0))
    }.toMap).value

    val cateConfBroadCast = sc.broadcast(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/CATCONF.txt")).getLines().map(_.split(",")).map {
      case Array(a, b) => (a, b)
    }.toMap).value
    def prepareCateCode(item: String): String = {
      val head = item.split(",")(0)
      val cateTrans = cateConfBroadCast.get(head.toUpperCase())
      if (cateTrans.isDefined) {
        s"${item.replace(head, cateTrans.get)},${head}"
      } else {
        s"${item}, "
      }
    }

    val sourceRDD = sc.textFile(args(0))
      .map(prepareCateCode)
      .map(_.split(","))
      .map(i => Item(i(0), i(1), s"${i(2)} ${i(3)} ${i(4)},${i(5)}".toUpperCase, s"${i(1)}".substring(0, 8), s"${i(1)}".substring(8, 13)))
      .filter(i => packBroadcast.keys.toSet.contains(i.cateCode.toUpperCase()))
      .map(i => (i.cateCode.toUpperCase(), i))

    val p = sourceRDD.map {
      case (category, item) => {
        import com.nielsen.packsize.PacksizeCoding
        val description = item.brandDescription
        val pack = packBroadcast.get(item.cateCode).get
        val b = pack.pack.split(";").map(replaceC2E).map(i => {
          (i, PacksizeCoding.getPackSize(description, i))
        }).maxBy {
          case (i, Some(d)) => d
          case (i, None) => -1
        }
        b._2 match {
          case Some(0) => (item, 1526, (b._1, None))
          case None => (item, 1526, b)
        }
      }
    }

    val result = p.map {
      case (item, code, (pack, Some(number))) => {
        val categoryString = s"${item.id},20,${categoryCacheList(item.cateCode)},${item.cateCode},${item.perCode},${item.storeCode}"
        s"${item.id},${code},${number}${pack},${number}${pack},${item.perCode},${item.storeCode}"
      }
      case (item, code, (i, None)) => {
        val categoryString = s"${item.id},20,${categoryCacheList(item.cateCode)},${item.cateCode},${item.perCode},${item.storeCode}"
        if (item.cateCode == "IMF") {
          val string = s"${item.id},${code},0G,0G,${item.perCode},${item.storeCode}"
          s"${string}${separator}${categoryString}"
        } else if (item.cateCode == "DIAP") {
          val string = s"${item.id},${code},0P,0P,${item.perCode},${item.storeCode}"
          s"${string}${separator}${categoryString}"
        } else {
          val string = s"${item.id},${code},UNKNOWN,UNKNOWN,${item.perCode},${item.storeCode}"
          s"${string}${separator}${categoryString}"
        }
      }
    }
    result.take(20).foreach(println)
  }
}