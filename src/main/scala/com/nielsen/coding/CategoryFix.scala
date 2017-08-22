package com.nielsen.coding

import org.apache.spark.{SparkConf, SparkContext}

/*
 * args:cateogry文件位置 newItem文件位置 修正文件位置 最终存储路径
 */
object CategoryFix {
  val split: String => Array[String] = _.split(",")
  val categoryItemKey: Array[String] => (String, Array[String]) = a => (a(1), a)
  val newItemKey: Array[String] => (String, Array[String]) = a => (a(15), a)
  val fixKey: Array[String] => ((String, String, String), Array[String]) = a => ((a(2), a(1), a(3)), a)

  def main(args: Array[String]): Unit = {
    val categoryUrl = args(0)
    val newItemUrl = args(1)
    val fixFileUrl = args(2)
    val output = args(3)
    val conf = new SparkConf()
    //conf.setAppName("categoryFix")
    //conf.setMaster("local[*]")
    val context = new SparkContext(conf)
    val categoryFile = context.textFile(categoryUrl).map(split andThen categoryItemKey)
    val newItemFile = context.textFile(newItemUrl).map(split andThen newItemKey)
    val fixItemFile = context.textFile(fixFileUrl).map(split andThen fixKey).collectAsMap()

    val joined = categoryFile.join(newItemFile).map(extractProductId)

    joined.sparkContext.broadcast(fixItemFile)
    val fixedCategory = joined.mapPartitions(iter => {
      iter.map {
        case (key, raw) =>
          fixItemFile.get(key) match {
            case None =>
              raw.mkString(",")
            case Some(v) =>
              raw(0) = v(4)
              raw.mkString(",")
          }
      }
    })

    fixedCategory.saveAsTextFile(output)
    context.stop()
  }

  def extractProductId(s: (String, (Array[String], Array[String]))): ((String, String, String), Array[String]) = {
    val newItem = s._2._2
    val categorySource = s._2._1
    val productId = newItem(3)
    val store = newItem(4)
    val description = newItem(12)
    ((productId, store, description), categorySource)
  }

}
