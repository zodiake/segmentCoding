package com.nielsen.outlier

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.shell.Delete

/**
 * @author daiyu01
 */
object OutlierCheck {
  def main(args: Array[String]): Unit = {
    if(args.length<3){
      System.err.println("Usage: <segment coding file> <new sales file> <output file>")
      System.exit(1)
    }
    
    val conf = new SparkConf()
    conf.setAppName("OutlierDetection")
 //   conf.setMaster("local")
    
    /*System.setProperty("hadoop.home.dir","D://bakFile//hadoop-2.6.0");
    System.setProperty("HADOOP_USER_NAME","wuke01");*/
    
    val sc = new SparkContext(conf)
    
    //val segFile = sc.textFile("C://Users//daiyu01//Desktop//JD_SEG").map { x =>  -- for test
    val segFile = sc.textFile(args(0)).map { x => 
      val field = x.split(",",-1)
      val periodCode = field(4)
      var cateCode = ""
      var brandCode = ""
      if(field(1) == "20")  cateCode = field(3)
      if(field(1) == "2126") brandCode = field(3)
      val mark = field(5)
      val itemId = field(0)
      (itemId,(periodCode,cateCode,brandCode,mark))
    }.filter(x => !(x._2._2 == "" && x._2._3 == "")).collect().toList
    
   val segFileRdd = sc.parallelize(segFile)
   val segFilePairRdd = new PairRDDFunctions(segFileRdd).reduceByKey((x,y) => funcReduce(x,y))
   
   
   //for test   (x._2._1._1,x._2._1._2,x._2._1._3,x._2._1._4,salesInfo._1.substring(0,10),x._1,salesInfo._2,salesInfo._3,salesInfo._4,salesInfo._5)  
   /*val segFileRdd1 = sc.textFile("C://Users//daiyu01//Desktop//SegFile_New").map { x => x.split(",",-1) }.map { x =>(x(0),(x(1),x(2),x(3),x(4)))}
   val segFilePairRdd = new PairRDDFunctions(segFileRdd1)*/
   // segFilePairRdd.collect().foreach(println(_))
   
   //val salesFile = sc.textFile("C://Users//daiyu01//Desktop//Sales_New").map { x =>   --for local test
   val salesFile = sc.textFile(args(1)).map { x =>  
     val sales = x.split(",",-1)
     val price = sales(4)
     val salesUnit = sales(2)
     val date = sales(5)
     val itemId = sales(6)
     val transCnt = sales(1)
     val salesValue = sales(3)
     val sellingPrice = sales(0)
     (itemId,(date,price,salesUnit,transCnt,salesValue,sellingPrice))
   }
   val finalRddPre = segFilePairRdd.leftOuterJoin(salesFile).map{x =>
     var salesInfo:(String,String,String,String,String,String) = ("","","","","","")
     if(x._2._2.get!=null)  salesInfo = x._2._2.get
     (x._2._1._1,x._2._1._2,x._2._1._3,x._2._1._4,salesInfo._1.substring(0,10),x._1,salesInfo._2,salesInfo._3,salesInfo._4,salesInfo._5,salesInfo._6)  
   }
   
    /*在做检测时是需要将下列促销日数据排除在外的：
                     双11（11月11号）
                     双12（12月12号）
      Cattype是'HB'类的 6-16（6月16号）
      Cattype不是'HB'类的 6-17（6月17号）*/
   val finalRdd = finalRddPre.filter(_._5.substring(5)!="11-11").filter(_._5.substring(5)!="12-12").filter{x=> !(x._2 =="HB" && x._5.substring(5)=="06-16")}.filter{x=> !(x._2 !="HB" && x._5.substring(5)=="06-17")}
   
   
   //transaction count by periodcode catecode brandcode mark
   val transCntRdd = finalRdd.map(x=>((x._1,x._2,x._3,x._4),1.toDouble))
   val transCntPairRdd = new PairRDDFunctions(transCntRdd).reduceByKey(_+_)
   
   val priceRdd = finalRdd.map(x=>((x._1,x._2,x._3,x._4),x._7))
   val unitRdd = finalRdd.map(x=>((x._1,x._2,x._3,x._4),x._8))
   
   //caculate the median price number by periodcode catecode brandcode mark
   val pricePairRdd = new PairRDDFunctions(priceRdd).groupByKey().map(x=>(x._1,funcGetMedian(x._2)))
   
   
   //caculate the median unit number by periodcode catecode brandcode mark
   val unitPairRdd = new PairRDDFunctions(unitRdd).groupByKey().map(x=>(x._1,funcGetMedian(x._2)))
   
   //caculate the RMSE for price
   val priceRMSE = priceRdd.leftOuterJoin(pricePairRdd).map{x=>
     var medianNum:Double = 0.0
     if(x._2._2.get!=null)  medianNum = x._2._2.get
    // println("the media price = " + medianNum)
     var priceSquare = Math.pow(x._2._1.toDouble - medianNum, 2)
     (x._1,priceSquare)
   }.reduceByKey(_+_).leftOuterJoin(transCntPairRdd).map{x=>
      var transCnt:Double = 1
      if(x._2._2.get!=null) transCnt = x._2._2.get
      var RMSE = Math.sqrt(x._2._1/transCnt)
      (x._1,(RMSE))
   }
   
   
   //caculate the RMSE for unit
   val unitRMSE = unitRdd.leftOuterJoin(unitPairRdd).map{x=>
     var medianNum:Double = 0.0
     if(x._2._2.get!=null)  medianNum = x._2._2.get
     var priceSquare = Math.pow(x._2._1.toDouble - medianNum, 2)
     (x._1,priceSquare)
   }.reduceByKey(_+_).leftOuterJoin(transCntPairRdd).map{x=>
      var transCnt:Double = 1
      if(x._2._2.get!=null) transCnt = x._2._2.get
      var RMSE = Math.sqrt(x._2._1/transCnt)
      (x._1,(RMSE))
   }
   
   
   /*
    * 比较price是否在以下区间:
    * High Price = Control Price + (6.5*RMSE)   
    * Low Price = Control Price – (6.5*RMSE)
    * N.B Control Price=Median Price
    */
   val priceControl = priceRMSE.leftOuterJoin(pricePairRdd).map{x=>
     var medianNum:Double = 0
     if(x._2._2.get!=null)  medianNum = x._2._2.get
     var highPrice = medianNum + 6.5*x._2._1
     var lowPrice = medianNum - 6.5*x._2._1
     (x._1,(highPrice,lowPrice))
   }
   
   /*
    * 比较unit是否在以下区间:
    * High Unit = Control Unit + (6*RMSE)   
    * Low Unit = Control Unit – (6*RMSE)
    * N.B Control Unit = Median Unit
    */
    val unitControl = unitRMSE.leftOuterJoin(unitPairRdd).map{x=>
     var medianNum:Double = 0
     if(x._2._2.get!=null)  medianNum = x._2._2.get
     var highUnit = medianNum + 6*x._2._1
     var lowUnit = medianNum - 6*x._2._1
     (x._1,(highUnit,lowUnit))
   }
   
   
   
   /*
    * Price 修正
    * • Rule 1 : If Low Price <= price <= High Price, price = no change
    * • Rule 2 : If High Price < price, price = High Price
    * • Rule 3 : If price < Low Price, price = Low Price
    * 
    * Unit 修正
    * • Rule 1 : If Low Salesunit <= salesunit <= High Salesunit, salesunit = no change
    * • Rule 2 : If High Salesunit < salesunit, salesunit = High Salesunit
    * • Rule 3 : If salesunit < Low Salesunit, salesunit = Low Salesunit  
    */
    
   val controllRdd = priceControl.join(unitControl)
   val dataRdd = finalRdd.map(x=>((x._1,x._2,x._3,x._4),(x._5,x._6,x._9,x._8,x._10,x._7,x._11)))
   val resultRdd = dataRdd.leftOuterJoin(controllRdd).map{x=>
       var controllCondition = x._2._2.get
       var highPrice = controllCondition._1._1
       var lowPrice = controllCondition._1._2
       var highUnit = controllCondition._2._1
       var lowUnit = controllCondition._2._2
       var realPrice = x._2._1._6.toDouble
       var realUnit = x._2._1._4.toDouble
       var adjustPrice = realPrice
       var adjustUnit = realUnit
       var adjustSalesValue = 0.0
       if(realPrice>highPrice) adjustPrice = highPrice
       if(realPrice<lowPrice) adjustPrice = lowPrice
       if(realUnit>highUnit) adjustUnit = highUnit
       if(realUnit<lowUnit) adjustUnit = lowUnit
       adjustSalesValue = adjustPrice*adjustUnit
     //  (x._1._1,x._1._2,x._1._3,x._1._4,x._2._1._1,x._2._1._2,x._2._1._7,x._2._1._3,x._2._1._4,x._2._1._5,x._2._1._6,highPrice,lowPrice,highUnit,lowUnit,adjustPrice,adjustUnit,adjustSalesValue)
     (x._2._1._2,x._2._1._1,x._2._1._3,x._2._1._4,x._2._1._5,x._2._1._6,x._2._1._7,x._1._1,adjustPrice,adjustUnit,adjustSalesValue,x._1._4,x._1._2,x._1._3)
   }.map(x => x._1+","+x._2+" 00:00:00"+","+x._7+","+x._3+","+x._4+","+x._5+","+x._6+","+x._8+","+x._9+","+x._10+","+x._11+","+x._12+","+x._13+","+x._14)
   //resultRdd.collect.foreach(println(_))
   deleteExistPath(args(2))
   resultRdd.saveAsTextFile(args(2))
  }
  def deleteExistPath(pathRaw:String){
    val outPut = new Path(pathRaw)
       val hdfs = FileSystem.get(URI.create(pathRaw),new Configuration())
       if(hdfs.exists(outPut)) hdfs.delete(outPut, true)
  }
  def funcReduce(x:(String,String,String,String),y:(String,String,String,String)):(String,String,String,String) ={
      var cateCode = ""
      var brandCode = ""
      if(x._2 != "") cateCode = x._2 else cateCode = y._2
      if(x._3 != "") brandCode = x._3 else brandCode = y._3
      return (x._1,cateCode,brandCode,x._4)
  }
  
  def funcGetMedian(iter:Iterable[String]):Double = {
    val list = iter.toList.map(x => x.toDouble).sorted
    var medianNum = 0.0
    val size = list.size
    if(size%2 == 1){
      medianNum = list.apply((size-1)/2)
    }else{
      medianNum = (list.apply(size/2)+list.apply(size/2-1))/2
    }
    
    return medianNum
  }
}