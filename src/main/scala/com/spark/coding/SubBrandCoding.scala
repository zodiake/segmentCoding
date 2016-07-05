package com.spark.coding

import com.nielsen.model.{IdAndKeyWordAndParentNo, Par, SegIdWithIndexAndSegName}
import com.spark.model.Item
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SubBrandCoding {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    conf.setAppName("TotalCoding")
    val sc = new SparkContext(conf)
    val segmentConfigRDD = sc.textFile("D:/wangqi/testFile/SEGCONF").map(_.split(",")).cache
    //val catList = segmentConfigRDD.map(_(1)).distinct.collect()
    val catList = List("BIS")

    val cateConf = sc.textFile("D:/wangqi/testFile/CATCONF").map(_.split(",")).map { case Array(a, b) => (a, b) }.collectAsMap
    val cateConfBroadCast = sc.broadcast(cateConf)

    val subBrandRDD = segmentConfigRDD.filter(_ (3).toUpperCase() == "SUBBRAND").cache
    val parentId = subBrandRDD.map(j => (j.head, j.last)).collectAsMap
    val parentIdBroadcast = sc.broadcast(parentId)

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

    val itemCacheList = segmentConfigRDD.map { i => (i(0), i) }.collectAsMap
    val itemCacheListBroadcast = sc.broadcast(itemCacheList)

    val subBrandConfigRDD = subBrandRDD.map(i => {
      val id = i(0)
      (i(1), List(IdAndKeyWordAndParentNo(id, i(5)), IdAndKeyWordAndParentNo(id, i(6)), IdAndKeyWordAndParentNo(id, i(8))))
    }).groupByKey
    val subBrand = subBrandConfigRDD.map(i => {

    })

    val join = sourceRDD.join(subBrandConfigRDD)

    val result = join.map {
      case (cateCode, (item, keywords)) => {
        val brandDesc = item.brandDescription
        lazy val first = keywords.map(i => i.head).toList.filter(i => i.keyWord != "" && i.keyWord != "其他牌子")
        lazy val second = keywords.map(i => i.drop(1).head).toList.filter(i => i.keyWord != "")
        lazy val third = keywords.map(i => i.drop(2).head).toList.filter(i => i.keyWord != "" && i.keyWord != "O.Brand")
        val p: Stream[List[IdAndKeyWordAndParentNo]] = Stream(first, second, third)

        def extractSubBrand(list: Stream[List[IdAndKeyWordAndParentNo]], result: => List[SegIdWithIndexAndSegName] = List[SegIdWithIndexAndSegName]()): List[SegIdWithIndexAndSegName] = {
          def go(list: List[IdAndKeyWordAndParentNo]): List[SegIdWithIndexAndSegName] = ???

          (list, result) match {
            case (Stream.Empty, _) => result
            case (_, h2 :: t2) => result
            case _ => extractSubBrand(list.tail, go(list.head) ++ result)
          }
        }

        val temp = extractSubBrand(p).map(i => IdDescIndexParent(i.segmentId, "", Nil, parentIdBroadcast.value.getOrElse(i.segmentId, "-")))
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
        b.toList.sortBy(i => (i.index.head, i.desc.length())) match {
          case Nil => {
            s"${item.id},3000,UNKNOWN,UNKNOWN,${item.perCode},${item.storeCode}"
          }
          case h :: t => {
            val c = itemCacheListBroadcast.value.get(h.id).get(10)
            s"${item.id},3000,${h.id},${c},${item.perCode},${item.storeCode}"
          }
        }
      }
    }
    result.take(10).foreach(println)
  }
}
