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

    val sourceRDD = sc.textFile("D:\\wangqi\\ml\\svm\\skin_928_1\\new").map(_.split(",")).map(i => (i(0).substring(1), i(1).substring(0, i(1).length - 1))).map(i => (i._2.toDouble.toInt, i._1))
    val mapping = sc.textFile("d:/wangqi/squirrel/cateogry_number_en").map(_.split(",")).map(i => (i(0).toInt, i(1)))
    sourceRDD.join(mapping).map(i => i._2._1 + "," + i._2._2).saveAsTextFile("d:/wangqi/ml/112")
  }
}
