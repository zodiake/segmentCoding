package com.nielsen.coding

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.rdd.PairRDDFunctions

/**
  * @author daiyu01
  */
object SkinSegCoding {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage: <segConf> <inputFilePath> <outputFilePath> <cateCode> <segNameLst>")
      System.exit(1)
    }

    val conf = new SparkConf
    // conf.setMaster("local")
    conf.set("spark.driver.maxResultSize", "10G")
    conf.setAppName("SkinSegCoding")
    val sc = new SparkContext(conf)
    val catCode = args(3) //args(3) "SKIN"
    val segNoLst = args(4).split(",").toList //args(4) "310"
    val segConf = sc.textFile(args(0)).map(_.split(",")).collect().toList.filter(_ (1) == catCode) //args(0) "C://Users//daiyu01//Desktop//config//SEGCONF_SKIN"
    //args(1)
    //判断传入的参数是否为一个文件夹，若是则往下读并且合并文件
    val hdfs = FileSystem.get(new Configuration)
    val path = new Path(args(1))
    val templist = Array[String]()
    val templistFinal = Array[String]()
    var ree = sc.parallelize(templist)
    val codUtil = new codingUtil

    if (hdfs.exists(path)) {
      val fileStatus = hdfs.listStatus(path)
      for (fileStatu <- fileStatus) {
        if (fileStatu.getPath.toString().indexOf("_SUCCESS") < 0) {
          val testFile = sc.textFile(fileStatu.getPath.toString()).map { x => (x.split(",")(0), x) } //"C://Users//daiyu01//Desktop//SkinTest"
          val segmentRddForTotal = new PairRDDFunctions(testFile).reduceByKey(_ + ";" + _) //List[(itemId,List[Segment])]
          val segmentRdd = segmentRddForTotal.map(x => (x._1, x._2.split(";").toList)).collect().toList
          val resultSrc = coding(segConf, segNoLst, sc, segmentRdd, catCode, codUtil)
          ree = sc.parallelize(resultSrc) ++ ree
        }
      }
    } else {
      throw new SparkException(
        "input file path is not exist, please check...")
    }


    /* if(hdfs.exists(path)){
         val fileStatus = hdfs.listStatus(path)
         for(fileStatu <- fileStatus){
             if(fileStatu.getPath.toString().indexOf("_SUCCESS")<0){*/
    //  val testFile = sc.textFile(args(1)).map { x => (x.split(",")(0),x) } //"C://Users//daiyu01//Desktop//SkinTest"
    /*val testFile = sc.textFile(args(1)).map { x => x.split(",") }
    val skinItemIdLst = testFile.filter { x => x(3) == "SKIN" }.map(_(0)).distinct().collect().toList
    val skinRdd = testFile.filter { x => skinItemIdLst.contains(x(0)) }.map { x => (x(0),x.mkString(",")) }
    val segmentRddForTotal = new PairRDDFunctions(skinRdd).reduceByKey(_+";"+_)//List[(itemId,List[Segment])]
    val segmentRdd = segmentRddForTotal.map(x=>(x._1,x._2.split(";").toList)).collect().toList
    val resultSrc = coding(segConf,segNoLst,sc,segmentRdd,catCode,codUtil)
    ree = sc.parallelize(resultSrc) */
    /*}
}
}else{
throw new SparkException(
  "input file path is not exist, please check...")
}*/

    //for avoid use reducebykey


    //for temp
    /*if(hdfs.exists(path)){
      val fileStatus = hdfs.listStatus(path)
      var reeFinal = sc.parallelize(templistFinal)
      for(fileStatu <- fileStatus){//外面一层
        if(fileStatu.isDirectory()){ //是文件夹 继续遍历
          var reeInner = sc.parallelize(templist)
          val innerFileStatus = hdfs.listStatus(fileStatu.getPath)
          for(innerFileStatu <- innerFileStatus){
            if(innerFileStatu.isFile()){
              if(innerFileStatu.getPath.toString().indexOf("_SUCCESS")<0){
                val testFile = sc.textFile(innerFileStatu.getPath.toString()).map { x => (x.split(",")(0),x) } //"C://Users//daiyu01//Desktop//SkinTest"
                val segmentRddForTotal = new PairRDDFunctions(testFile).reduceByKey(_+";"+_)//List[(itemId,List[Segment])]
                val segmentRdd = segmentRddForTotal.map(x=>(x._1,x._2.split(";").toList)).collect().toList
                val resultSrc = coding(segConf,segNoLst,sc,segmentRdd,catCode,codUtil)
                reeInner = sc.parallelize(resultSrc) ++ reeInner
              }
            }else{
              throw new SparkException("the path structure is not correct, please check...")
            }
          }
          reeFinal = reeInner ++ reeFinal
        }else{//文件 则做coding
          if(fileStatu.getPath.toString().indexOf("_SUCCESS")<0){
              val testFile = sc.textFile(fileStatu.getPath.toString()).map { x => (x.split(",")(0),x) } //"C://Users//daiyu01//Desktop//SkinTest"
              val segmentRddForTotal = new PairRDDFunctions(testFile).reduceByKey(_+";"+_)//List[(itemId,List[Segment])]
              val segmentRdd = segmentRddForTotal.map(x=>(x._1,x._2.split(";").toList)).collect().toList
              val resultSrc = coding(segConf,segNoLst,sc,segmentRdd,catCode,codUtil)
              reeFinal = sc.parallelize(resultSrc) ++ reeFinal
           }
        }
      }
      
    codUtil.deleteExistPath(args(2))
    reeFinal.filter(_!="").saveAsTextFile(args(2))
      
    }else{
      throw new SparkException("input file path is not exist, please check...")
    }*/


    /* val array = Array[Int](33,34,35)
         for(i <- array){
               val path = new Path(args(1)+"SKIN_ALL_"+i+".SEG")
               var ree1 = sc.parallelize(templist)
               if(hdfs.exists(path)){
                 val fileStatus = hdfs.listStatus(path)
                 for(fileStatu <- fileStatus){
                   if(fileStatu.getPath.toString().indexOf("_SUCCESS")<0){
                     val testFile = sc.textFile(fileStatu.getPath.toString()).map { x => (x.split(",")(0),x) } //"C://Users//daiyu01//Desktop//SkinTest"
                     val segmentRddForTotal = new PairRDDFunctions(testFile).reduceByKey(_+";"+_)//List[(itemId,List[Segment])]
                     val segmentRdd = segmentRddForTotal.map(x=>(x._1,x._2.split(";").toList)).collect().toList
                     val resultSrc = coding(segConf,segNoLst,sc,segmentRdd,catCode,codUtil)
                     ree1 = sc.parallelize(resultSrc) ++ ree1
                   }
                 }
               codUtil.deleteExistPath(args(2)+"_"+i)
               ree1.filter(_!="").saveAsTextFile(args(2)+"_"+i)
         }
    }*/
    codUtil.deleteExistPath(args(2))
    ree.repartition(64).filter(_ != "").saveAsTextFile(args(2))
  }

  def coding(segConf: List[Array[String]], segNoLst: List[String], sc: SparkContext, segmentRddForTotal: List[(String, List[String])], catCode: String, codUtil: codingUtil): Array[String] = {
    //filter 
    val segmentRddForCatCode = segmentRddForTotal.filter { x => function1(x._2, catCode) }.map { x => (x._1, x._2.filter { segment => !segNoLst.contains(segment.split(",")(1)) }) } //filter out the segment which is equals segNolst

    val catCodeItemIdLst = segmentRddForCatCode.map(_._1)


    val segmentRddExceptCatCode = sc.parallelize(segmentRddForTotal.filter { ele => !catCodeItemIdLst.contains(ele._1) })
    var item_result = List[(String, List[String])]()
    for (segNo <- segNoLst) {
      val codingConf = itemmaster_segment(segNo, segConf)
      val segNoNeedToCombineLst = codUtil.getSegNoForCombine(codingConf.head._2)
      val segNeedToAdd = segmentRddForCatCode.map { segment =>
        val segArr = segment._2.head.split(",")
        val segCombineDesc = function2(segment._2, segNoNeedToCombineLst)
        var segCombineId = codUtil.getCombineSegId(segCombineDesc, codingConf)
        var segcode = "UNKNOWN"
        if (segCombineId == "") {
          segCombineId = "UNKNOWN"
        } else {
          segcode = segConf.filter(_ (0) == segCombineId).map(_ (10)).head
        }
        item_result = (segment._1, List(segment._1 + "," + segNo + "," + segCombineId + "," + segcode + "," + segArr(4) + "," + segArr(5))) :: item_result
      }
    }

    val resultForCateCode = item_result ::: segmentRddForCatCode
    val resultRdd = new PairRDDFunctions(sc.parallelize(resultForCateCode)).reduceByKey(_ ::: _) ++ segmentRddExceptCatCode
    val resultStr = resultRdd.map(_._2.mkString("\n")).collect
    return resultStr
  }


  def function1(segmentLst: List[String], catCode: String): Boolean = {
    var flag = false
    for (seg <- segmentLst) {
      if (seg.split(",")(3) == catCode) {
        flag = true
      }
    }
    return flag
  }

  def function2(segmentLst: List[String], relatedSegNoLst: List[String]): List[String] = {
    var segCombineDesc = List[String]()
    segCombineDesc = relatedSegNoLst.map { segNo => segNo + "-" + function3(segmentLst, segNo) }
    return segCombineDesc
  }

  def function3(segmentLst: List[String], segNo: String): String = {
    var str = ""
    for (seg <- segmentLst) {
      if (seg.split(",")(1) == segNo) {
        str = seg.split(",")(3)
      }
    }
    return str
  }

  def itemmaster_segment(segNo: String, segConf: List[Array[String]]): List[(String, String)] = {
    val segLst = segConf.filter(x => x(2) == segNo)
    val desc = segLst.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "")
    return desc
  }
}