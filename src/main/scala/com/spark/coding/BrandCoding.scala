package com.spark.coding

import com.nielsen.model.{IdAndKeyWord, Par, SegIdWithIndexAndSegName}
import com.spark.model.Item
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

case class IdDescIndexParent(id: String, desc: String, index: List[Int], parentId: String)

/*
 *  args(0) segmentConfig
 *  args(1) sourceFile
 *  args(2) catConfig file
 *  args(3) output file
 */
object BrandCoding {

  def filterParentId(brandDesc: String, extract: => List[SegIdWithIndexAndSegName]) = {
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
    val conf = new SparkConf().setMaster("local").setAppName("BrandCoing")
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
    val sourceRDD = sc.textFile("D:/wangqi/testFile/TMTB.catcoded")
      .map(prepareCateCode)
      .map(_.split(","))
      .map(i => Item(i(0), i(1), s"${i(2)} ${i(3)} ${i(4)}", s"${i(1)}".substring(0, 8), s"${i(1)}".substring(8, 13)))
      .map(i => (i.cateCode.toUpperCase(), i))
      .partitionBy(new HashPartitioner(100))
    val segmentRDD = segmentConfigRDD.filter(_ (3).toUpperCase() == "BRAND").cache

    val itemCacheList = segmentConfigRDD.map { i => if (i.length < 10) {
      println(i(0)); (i(0), i(10))
    } else {
      (i(0), i(10))
    }
    }.collectAsMap
    val itemCacheListBroadcast = sc.broadcast(itemCacheList)

    val brandNo = segmentConfigRDD.filter(_ (3) == "BRAND").take(1).map(_ (2)).head
    val brandConfig = segmentRDD.map(i => {
      val id = i(0)
      (i(1), List(IdAndKeyWord(id, i(5), i.last), IdAndKeyWord(id, i(6), i.last), IdAndKeyWord(id, i(7), i.last), IdAndKeyWord(id, i(8), i.last), IdAndKeyWord(id, i(9), i.last)))
    }).groupByKey()

    val brand = brandConfig.map(i => {
      val list = i._2
      lazy val first = list.map(j => j.head).toList.filter(j => j.keyWord != "" && j.keyWord != "其他牌子")
      lazy val second = list.map { j => j.drop(1).head }.toList.filter { j => j.keyWord != "" }
      lazy val third = list.map(j => j.drop(2).head).toList.filter(j => j.keyWord != "" && j.keyWord != "其他厂家")
      lazy val fourth = list.map(j => j.drop(3).head).toList.filter(j => j.keyWord != "" && j.keyWord != "O.Brand")
      lazy val fifth = list.map(j => j.drop(4).head).toList.filter((j => j.keyWord != "" && j.keyWord != "O.Manu"))
      (i._1, Stream(first, second, third, fourth, fifth))
    })

    val joined = sourceRDD.join(brand)

    val result = joined.map {
      case (cateCode, (item, p)) => {
        val brandDesc = item.brandDescription

        def extractBrand(list: Stream[List[IdAndKeyWord]], result: List[SegIdWithIndexAndSegName] = List[SegIdWithIndexAndSegName]()): List[SegIdWithIndexAndSegName] = {
          def go(list: List[IdAndKeyWord]): List[SegIdWithIndexAndSegName] = {
            list.map(Par.parse(_)(brandDesc)).filter(i => i.index != Nil)
          }

          (list, result) match {
            case (Stream.Empty, _) => result
            case (_, h2 :: t2) => result
            case _ => extractBrand(list.tail, go(list.head) ++ result)
          }
        }
        val b = filterParentId(brandDesc, extractBrand(p))

        b.toList.sortBy(i => (i.index.head, i.desc.length())) match {
          case Nil => {
            s"${item.id},${brandNo},TAOBAO_ZZZOTHER,TAOBAO_ZZZOTHER,${item.perCode},${item.storeCode}"
          }
          case h :: t => {
            val c = itemCacheListBroadcast.value.get(h.id).get
            s"${item.id},${brandNo},${h.id},${c},${item.perCode},${item.storeCode}"
          }
        }
      }
    }
    result.collect()
  }
}