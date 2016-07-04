package com.spark.coding

import com.nielsen.model.{IdAndKeyWord, Par, SegIdWithIndexAndSegName}
import com.spark.model.Item
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

case class IdDescIndexParent(id: String, desc: String, index: List[Int], parentId: String)

/*
 *  args(0) sourceFile
 *  args(1) output file
 */
object BrandCoding {

  def filterParentId(brandDesc: String, extract: List[SegIdWithIndexAndSegName]) = {
    val temp = extract.map(i => IdDescIndexParent(i.segmentId, i.segmentName, i.index, i.parentNo))

    val parentId = temp.map(i => i.parentId).toSet.filter { i => i == "-" }

    val filterParentList = for {
      i <- temp if (!parentId.contains(i.id))
    } yield i

    val b = filterParentList.to[ListBuffer]

    for (i <- b) {
      for (j <- b) {
        if (j.desc.indexOf(i.desc) > -1 && j.desc.size > i.desc.size) {
          b -= i
        } else
          b
      }
    }
    b
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    val conf = new SparkConf().setMaster("local[2]").setAppName("BrandCoing")
    conf.set("spark.executor.memory", "5G")
    //val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.scheduler.mode", "FAIR")
    conf.registerKryoClasses(Array(classOf[IdAndKeyWord], classOf[SegIdWithIndexAndSegName]))
    val sc = new SparkContext(conf)
    val segConfig = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/SEGCONF.txt")).getLines().map(_.split(",")).toList
    val brandConfig = segConfig.filter(_ (3).toUpperCase == "BRAND")
    val categoryConfig = segConfig.filter(_ (3).toUpperCase == "CATEGORY")
    val separator = sys.props("line.separator")

    val itemCacheList = sc.broadcast(brandConfig.map { i =>
      (i(0), i(10))
    }.toMap).value

    val categoryCacheList = sc.broadcast(categoryConfig.map {
      i => (i(1).toUpperCase(), i(0))
    }.toMap).value


    val c = sc.broadcast(brandConfig.groupBy(_ (1)).map {
      case (key, value) => {
        val set = value.map(i => IdAndKeyWord(i(0), i(5), i.last))
        (key, set)
      }
    }).value

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
      .filter(_.size > 2)
      .map(i => Item(i(0), i(1), s"${i(2)} ${i(3)} ${i(4)}", s"${i(1)}".substring(0, 8), s"${i(1)}".substring(8, 13)))
      .map(i => (i.cateCode.toUpperCase(), i))

    val result = sourceRDD.filter(i => c.keys.toSet.contains(i._2.cateCode)).map {
      case (cateCode, item) => {
        val brandDesc = item.brandDescription

        def extractBrand(list: List[IdAndKeyWord]): List[SegIdWithIndexAndSegName] = {
          list.map(Par.parse(_)(brandDesc)).filter(i => i.index != Nil)
        }
        val b = filterParentId(brandDesc, extractBrand(c.get(item.cateCode).get))

        val re = s"${item.id},20,${categoryCacheList(item.cateCode)},${item.cateCode},${item.perCode},${item.storeCode}"

        b.toList.sortBy(i => (i.index.head, i.desc.length())) match {
          case Nil => {
            val string = s"${item.id},2126,TAOBAO_ZZZOTHER,TAOBAO_ZZZOTHER,${item.perCode},${item.storeCode}"
            s"${string}${separator}${re}"
          }
          case h :: t => {
            val c = itemCacheList.get(h.id).get
            val string = s"${item.id},2126,${h.id},${c},${item.perCode},${item.storeCode}"
            s"${string}${separator}${re}"
          }
        }
      }
    }

    //result.saveAsTextFile(args(1))
    result.take(10).foreach(println)
  }
}