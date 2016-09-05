import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 24/6/2016.
  */
object Sample {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val sourceRDD = sc.textFile("hdfs://hkgrherfpp001:9000/CATEGORY_CODED_DATA/TMTB/20161407/TMTB.catcoded_1")
    sourceRDD.take(1).foreach(println)

    /*
    val s = sourceData.map(_._2).flatMap(i => i.map(j => (j, 1))).reduceByKey(_ + _).filter(_._2 == 1).keys.collect()
    val termsCount = sourceData.map(_._2).flatMap(i => i.map(j => (j, 1))).reduceByKey(_ + _).filter(_._2 != 1).keys.count()
    val re = sourceData.map(_._2).flatMap(i => i.map(j => (j, 1))).reduceByKey(_ + _)

    val termTrain = sc.textFile("d:/wangqi/sock.train").map(_.split(",")).map(array => array(1)).map(_.split("/")).collect().flatten
    val termTest = sc.textFile("d:/wangqi/sock.test").map(_.split(",")).map(array => array(1)).map(_.split("/")).collect.flatten
    val number = termTest.filter(i => termTrain.contains(i))
    println(termTrain.size)
    println(number.size)
    */
  }
}
