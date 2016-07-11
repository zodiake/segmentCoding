package com.spark.coding

import com.spark.model.Item
import org.apache.spark.{SparkConf, SparkContext}

case class Pack(id: String, pack: String, name: String)

object PackSizeCode {
  def main(args: Array[String]): Unit = {
    val separator = sys.props("line.separator")
    System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    val conf = new SparkConf().setMaster("local[2]")
    conf.setAppName("PackCoding")
    val sc = new SparkContext(conf)

    val (packConfig, categoryConfig) = SegmentUtils.getConfig("PACKSIZE")

    val packBroadcast = sc.broadcast(packConfig.map {
      i => (i(1), Pack(i(2), i(5), i(10)))
    }.toMap).value

    val categoryCacheList = sc.broadcast(categoryConfig.map {
      i => (i(1).toUpperCase(), i(0))
    }.toMap).value

    val cateConfBroadCast = sc.broadcast(SegmentUtils.getConfigFile("/CATCONF.txt").getLines().map(_.split(",")).map {
      case Array(a, b) => (a, b)
    }.toMap).value

    val sourceRDD = sc.textFile(args(0))
      .map(i => SegmentUtils.prepareItem(i, cateConfBroadCast))
      .map(_.split(","))
      .map(i => Item(i(0), i(1), s"${i(2)} ${i(3)} ${i(4)},${i(5)}".toUpperCase, s"${i(1)}".substring(0, 8), s"${i(1)}".substring(8, 13)))
      .filter(i => packBroadcast.keys.toSet.contains(i.cateCode.toUpperCase()))
      .map(i => (i.cateCode.toUpperCase(), i))

    val p = sourceRDD.map {
      case (category, item) => {
        import com.nielsen.packsize.PacksizeCoding
        val description = item.brandDescription
        val pack = packBroadcast.get(item.cateCode).get
        val b = pack.pack.split(";").map(SegmentUtils.replaceC2E).map(i => {
          (i, PacksizeCoding.getPackSize(description, i))
        }).maxBy {
          case (i, Some(d)) => d
          case (i, None) => -1
        }
        b._2 match {
          case Some(e) => if (e == 0) (item, 1526, (b._1, None)) else (item, 1526, (b._1, Some(e)))
          case None => (item, 1526, b)
        }
      }
    }

    val result = p.map {
      case (item, code, (pack, Some(number))) => {
        val categoryString = s"${item.id},20,${categoryCacheList(item.cateCode)},${item.cateCode},${item.perCode},${item.storeCode}"
        val string = s"${item.id},${code},${number}${pack},${number}${pack},${item.perCode},${item.storeCode}"
        s"${string}${separator}${categoryString}"
      }

      case (item, code, (i, None)) => {
        val categoryString = s"${item.id},20,${categoryCacheList(item.cateCode)},${item.cateCode},${item.perCode},${item.storeCode}"
        item.cateCode match {
          case "IMF" =>
            val string = s"${item.id},${code},0G,0G,${item.perCode},${item.storeCode}"
            s"${string}${separator}${categoryString}"
          case "DIAP" =>
            val string = s"${item.id},${code},0P,0P,${item.perCode},${item.storeCode}"
            s"${string}${separator}${categoryString}"
          case _ =>
            val string = s"${item.id},${code},UNKNOWN,UNKNOWN,${item.perCode},${item.storeCode}"
            s"${string}${separator}${categoryString}"
        }
      }
    }

    //result.saveAsTextFile(args(1))
    result.take(20).foreach(println)
  }
}