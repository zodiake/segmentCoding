package com.spark

import java.text.SimpleDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Test {
   def main(args: Array[String]){
    /*if (args.length < 2) {
      System.err.println("Usage: <inputFile> <outputFile> <brand_list>")
      System.exit(1)
    }*/
    
    System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    conf.setMaster("local").setAppName("SparkTest1")
    val sc = new SparkContext(conf)

     //C:\Users\Mirra\Desktop
    val configfile = sc.textFile("C://Users//wangqi08//Desktop//SEGCONF").map(_.split(",")).collect().toList.filter(_(1) == "DIAP")
    val catcode = "DIAP"
 //   val configfile1 = sc.textFile("C://Users//daiyu01//Desktop//SEGCONF").map(_.split(",")).map { x => (x(0),x(1)) }
    val configFileNew = configfile.filter(_(3)!="CATEGORY")
    val seglist = configfile.filter(x => x(3) != "BRAND" && !x(3).contains("SUBBRAND") && x(3) != "PACKSIZE" && x(3) != "PRICETIER" && x(3) != "CATEGORY")
                  .map(_(3)).distinct
    val a = sc.parallelize(List((1,2),(3,4),(3,6)))
    //brand coding
    val brand_conf = itemmaster_brand(catcode,configFileNew)
    val cateCodeCombine = "SKIN"
    val segCombine = "WHITEUV"
    
    println("========brand conf========")
    println(brand_conf)
   // brand_conf.foreach(_.foreach(println(_)))
    val description = "花王/妙而舒【天猫超市】日本原装进口花王纸尿裤 HUGGIES片9-14kg 正品行货系列:三倍透气||规格:54片||产地:日本||包装数量(片):54||尿不湿品类:纸尿裤||尿片规格型号:LMXXL  s168||适合体重:9KG-14KG||适用性别:通用||"
    //val description = "ABC ABC 甜睡夜用 超极薄 棉柔排湿表层 卫生巾 3片装 K34"
    //val description = "爱丝卡尔 滋养洗护套装（油性发质适用）洗发水500ml 护发素500ml 送180g洗"
    val codingFuct = new codingFunc()
    val codingUtil = new codingUtil()
    val brand_id = codingFuct.MKWLC(description, brand_conf)
    if(brand_id != ""){
      val brandno = configFileNew.filter(_(3) == "BRAND").map(_(2)).distinct.head // brand no
      val segcode = configFileNew.filter(_(0) == brand_id).map(_(10)).head
    }else {
      val brandno = configFileNew.filter(_(3) == "BRAND").map(_(2)).distinct.head
      println("==============")
    }
    println("brand_id:"+brand_id)

   val subbrand_namelist = configFileNew.filter(_(3).contains("SUBBRAND")).map(_(3)).distinct
   println("subbrand_namelist : " + subbrand_namelist)
     for(subbrand_name <- subbrand_namelist){
      val subbrand_conf = itemmaster_subbrand(catcode, configFileNew.filter(_.reverse.head == brand_id), subbrand_name)
      if(!subbrand_conf.isEmpty){
        println("============subbrand_conf==========")
        subbrand_conf.foreach(println(_))
        val subbrand = codingFuct.MKWLC(description, subbrand_conf)
       
        if(subbrand != ""){
          val subbrandno = configFileNew.filter(_(3) == subbrand_name).map(_(2)).distinct.head
          val segcode = configFileNew.filter(_(0) == subbrand).map(_(10)).head
           println("subbrand: " + subbrand)
          // item_result = (item.ITEMID + "," + subbrandno + "," + item.subbrand_id + "," + segcode + "," + item.perCode + "," + item.storeCode ) :: item_result
        }else if(!configFileNew.filter(_(3) == subbrand_name).isEmpty){
          val subbrandno = configFileNew.filter(_(3) == subbrand_name).map(_(2)).distinct.head
          println("subbrand: " + subbrand)
          // item_result = (item.ITEMID + "," + subbrandno + "," + "UNKNOWN"+ "," + "UNKNOWN" + "," + item.perCode + "," + item.storeCode) :: item_result
        }
      }
     }
    
    val packsize_conf = itemmaster_packsize(catcode, configFileNew)
    println("===========packsize coding================")
    println(packsize_conf)
    var packResult = "" 
    val test = packsize_conf.map(y =>
      (if (!codingFuct.PacksizeCoding(description, y, List()).isEmpty) {
        codingFuct.PacksizeCoding(description, y, List()).max.toString() //两个相同单位取了最大的
      } else { "" }, y)).filter(_._1 != "").filter(_._1 != "0.0")
        println(test)
    val packsize = packsize_conf.map(y =>
      (if (!codingFuct.PacksizeCoding(description, y, List()).isEmpty) {
        codingFuct.PacksizeCoding(description, y, List()).max.toString() //两个相同单位取了最大的
      } else { "" }, y)).filter(_._1 != "").filter(_._1 != "0.0").map(x => codingFuct.packsizetransform(x))
      .distinct.sortBy(_._1.toDouble).reverse.map(x => x._1 + x._2)
    if(!packsize.isEmpty){
      packResult = packsize.head//所有单位之间区最大的 
    }else { 
      packResult = ""
    }
    if(packResult != ""){
      val packsizeno = configFileNew.filter(_(3) == "PACKSIZE").map(_(2)).distinct.head
      val segcode = configFileNew.filter(_(1) == catcode).filter(_(3).toUpperCase() == "PACKSIZE").map(_(10)).head
      println("packsize : " + packResult + " segcode : " + segcode)
    }else{
      var packsizeno = ""
      if(!configFileNew.filter(_(3) == "PACKSIZE").map(_(2)).isEmpty){
        packsizeno = configFileNew.filter(_(3) == "PACKSIZE").map(_(2)).distinct.head
         if(catcode=="IMF"){
           println("packsize : " + "0G" + " segcode : " + "0G")
         }else{
           println("packsize : " + "UNKNOWN" + " segcode : " + "UNKNOWN")
         }
       }
     }
      
    
    println("=============seglist==============")
    var filterSegLst:List[String] = List()
     var finalSeglst:List[String] = List()
          if(catcode == cateCodeCombine){
            if(segCombine.contains(",")) filterSegLst = segCombine.split(",").toList else filterSegLst = List(segCombine)
            finalSeglst = seglist.filter { x => !filterSegLst.contains(x) }
          }else{
            finalSeglst = seglist
          }
          println(finalSeglst)
     for(seg <- finalSeglst) {
        //segment coding
          //for new requirement  combine two segment to one segment
         
        if(catcode == "FB" && seg == "CAPACITY"){
         /* val capacity_conf = configFileNew.filter(_(3) == "CAPACITY")
         // val capacity_result = capacity_conf.filter(x => codingFuct.checkprice(item.packsize.replace("ML",""), x(5)))
          if(capacity_result.isEmpty || capacity_result.size > 1 ){
  
          }else {
            val capacityno = capacity_result.head(2)
            item.capacity = capacity_result.head(0)
            val segcode = configFileNew.filter(_(0) == item.capacity).map(_(10)).head
            item_result = (item.ITEMID + "," + capacityno + "," + item.capacity+ "," + segcode + "," + item.perCode + "," + item.storeCode) :: item_result
          }
  */
        }else {
          var seg_id =""
          var other_id = List[(String,String)]()
          if(catcode=="IMF" && seg =="STAGE" ){
            /*val seg_conf = itemmaster_segment_forFirst(item_raw(0), seg, configFileNew)
            other_id = seg_conf.map(_.filter(_._2.toUpperCase() == "其他")).head
            seg_id = codingFunc.MKWLC(item.description, seg_conf)
            if(seg_id ==""){
              val seg_conf = itemmaster_segment_forSecond(item_raw(0), seg, configFileNew)
              other_id = seg_conf.map(_.filter(_._2.toUpperCase() == "其他")).head:::other_id
              seg_id = codingFunc.MKWLC(item.description, seg_conf)
            }
            if(seg_id == ""){
              val seg_conf = itemmaster_segment_forThird(item_raw(0), seg, configFileNew)
              other_id = seg_conf.map(_.filter(_._2.toUpperCase() == "其他")).head:::other_id
              seg_id = codingFunc.MKWLC(item.description, seg_conf)
            }
            if(seg_id == ""){
              seg_id="650006"
            }
            //Remap the “Pre段” to “1段” and ”6段” to  “3段” in Aptamil & Nutrilon
            if((item.brand_id == "630069"||item.brand_id == "630853"||item.brand_id == "630075") && seg == "STAGE"){
              if(seg_id=="650004" && (item.description.contains("6段")||item.description.contains("六段"))){
                seg_id = "650003"
              }
            }*/
          }
         
          else{
            val seg_conf = itemmaster_segment(catcode, seg, configFileNew)
            println("===========segment conf================")
            println(seg_conf)
            
          //  other_id = seg_conf.map(_.filter(_._2.toUpperCase() == "其他")).head
            other_id = seg_conf.map(_.filter(x=>x._2.toUpperCase() == "其他"||x._2.toUpperCase() == "其他其他")).head
            seg_id = codingFuct.MKWLC(description, seg_conf)
            if(seg_id != ""){
              
              var segcodeLst = configFileNew.filter(_(0) == seg_id).map(_(10)).head
              println("otherid: " + other_id)
              println("segName: "+ seg+ "  segId : " + seg_id + "   segType : " + segcodeLst)
            }else{
              seg_id = "UNKNOWN"
              println("segName: "+ seg+ "  segId : " + seg_id    )
            }
            
          }
         /* if(seg_id != ""){
            item.updateDynamic(seg)(seg_id)
          }else if(!other_id.isEmpty){
            item.updateDynamic(seg)(other_id.map(_._1).head)
          }else {
            item.updateDynamic(seg)("UNKNOWN")
          }
          if(item.selectDynamic(seg) != ""){
            val segno = configFileNew.filter(_(3) == seg).map(_(2)).distinct.head
            var segcodeLst = configFileNew.filter(_(0) == item.selectDynamic(seg)).map(_(10))
            var segcode = "UNKNOWN"
            if(!segcodeLst.isEmpty){
              segcode = segcodeLst.head
            }
            item_result = (item.ITEMID + "," + segno + "," + item.selectDynamic(seg)+ "," + segcode + "," + item.perCode + "," + item.storeCode) :: item_result
          }*/
        }
      }

    
 }
  
   def itemmaster_packsize(catcode: String, itemfile: List[Array[String]]): List[String] = {
    val itemlist = itemfile.filter(_(1) == catcode).filter(_(3).toUpperCase() == "PACKSIZE")
    if (!itemlist.isEmpty) {
      val desc = itemlist.map(_(5).toUpperCase())
        .filter(_ != "")
        .mkString(";")
        .split(";")
        .toList
      return desc
    } else return List()
  }
  
  def itemmaster_subbrand(catcode: String, itemfile: List[Array[String]], subbrand_name:String): List[List[(String, String)]] = {
    val itemlist = itemfile.filter(_(1) == catcode).filter(_(3).toUpperCase() == subbrand_name)
    val eccbrandlist = itemlist.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "" && x._2 != "其他牌子")

    val eccshortdesc = itemlist.map(x => (x.head, x(6).toUpperCase()))
      .filter(x => x._2 != "")

    val ecbrandlist = itemlist.map(x => (x.head, x(8).toUpperCase()))
      .filter(x => x._2 != "" && x._2 != "O.Brand")

    val parentidlist = itemlist.map(x => (x.head, x.reverse.head))

    return List(eccbrandlist, eccshortdesc, ecbrandlist, parentidlist)
  }
  
 def strTONum(str:String):Boolean={
   var num = 0.0 
   val pattern = "^(([0-9]+.[0-9]*[1-9][0-9]*)|([0-9]*[1-9][0-9]*.[0-9]+)|([0-9]*[1-9][0-9]*))$".r
   if(str != null && str !=""){
     val numbers = pattern.findAllIn(str).toList.mkString("")
     if(numbers == str){
       true
     }else{
       false
     }
   }else {
     false
   }
  }
  
   def strTODate(str:String):Boolean={
     val sdf = new SimpleDateFormat("yyyy-MM-dd")
     try {
       val date = sdf.parse(str)
       true
     } catch {
       case t: Throwable => false // TODO: handle error
     }
   }
 
 
   def itemmaster_brand(catcode: String, itemfile: List[Array[String]]): List[List[(String, String)]] = {
    val itemlist = itemfile.filter(_(1) == catcode).filter(_(3).toUpperCase() == "BRAND")
    val eccbrandlist = itemlist.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "" && x._2 != "其他牌子")

    val eccshortdesc = itemlist.map(x => (x.head, x(6).toUpperCase()))
      .filter(x => x._2 != "")

    val eccmanulist = itemlist.map(x => (x.head, x(7).toUpperCase()))
      .filter(x => x._2 != "" && x._2 != "其他厂家")

    val ecbrandlist = itemlist.map(x => (x.head, x(8).toUpperCase()))
      .filter(x => x._2 != "" && x._2 != "O.Brand")

    val ecmanulist = itemlist.map(x => (x.head, x(9).toUpperCase()))
      .filter(x => x._2 != "" && x._2 != "O.Manu")

    val parentidlist = itemlist.map(x => (x.head, x.reverse.head))

    return List(eccbrandlist, eccshortdesc, eccmanulist, ecbrandlist, ecmanulist, parentidlist)
  }
  
  
  
   private def multifindDesc(itemdesc: String, targetdesc: String): Int = {
    var flag = true
    val index = targetdesc.indexOf("/{")
    if(targetdesc.indexOf("/{")>=0){
      if(targetdesc.indexOf(";")>=0){
       val lst = targetdesc.split(";").filter(_.indexOf("/{")>=0).map(x => findNagtiveDesc(itemdesc,x))
       if(lst.min>=0){
         flag = false
         return -2
       }
      }else {
        val tmp = findNagtiveDesc(itemdesc,targetdesc)
        if(tmp>=0){
          flag = false
          return -2
          
        }
      }
    }
    
    if(flag){
      if (targetdesc.indexOf(";") >= 0) { //如果存在分号分割的字符串只要存在一个
      val targetlist = targetdesc.replace("/{", ";").replace("/", ";").replace("}", ";").split(";").map(x => findDesc(itemdesc, x)).toList // return the index of each string spilt by ";"
      if (targetlist.exists { _ > -1 }) {
        targetlist.filter(_ > -1).min
      } else
        -1
    } else
      findDesc(itemdesc, targetdesc)
    }else {
      -1
    }
  }

  def itemmaster_segment(catcode: String, segment: String, itemfile: List[Array[String]]): List[List[(String, String)]] = {
    val itemlist = itemfile.filter(_(1) == catcode).filter(x => x(3).toUpperCase() == segment)
    val desc = itemlist.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "")

    val parentidlist = itemlist.map(x => (x.head, x.reverse.head))

    return List(desc, parentidlist)
  }
   
  def findNagtiveDesc(itemdesc: String, targetdesc: String):Int = {
    val targetlist = targetdesc.replace("/{", "|").split("\\|").toList.map(_.replace("}", "")).map(x=>findDesc1(itemdesc,x))
    if(targetlist.min>=0){
       targetlist.min
    }else {
      -1
    }
  }
  
   private def findDesc1(itemdesc: String, targetdesc: String): Int = {
    if (targetdesc.indexOf("/") >= 0) { //如果存在斜杠分割的字符串则逐个寻找
      val targetlist = targetdesc.split("\\/").toList.map(x => itemdesc.indexOf(x))
      if (targetlist.max>=0) { //如果每个分割的字符串有一个存在则返回当前的位置
        targetlist.max
      } else
        -1
    } else
      itemdesc.indexOf(targetdesc) //没有斜杠的字符串按正常方式查找
  }
  
  private def findDesc(itemdesc: String, targetdesc: String): Int = {
    if (targetdesc.indexOf("/") >= 0) { //如果存在斜杠分割的字符串则逐个寻找
      val targetlist = targetdesc.split("/").toList.map(x => itemdesc.indexOf(x))
      if (targetlist.min >= 0) { //如果每个分割的字符串都能找到则返回最前面的位置
        targetlist.min
      } else
        -1
    } else
      itemdesc.indexOf(targetdesc) //没有斜杠的字符串按正常方式查找
  }
  
  def transCateCode(item:String,cateConf:List[Array[String]]):String = {
    val cateTrans = cateConf.filter(x=>x(0).equalsIgnoreCase(item.split(",")(0)))
    if(!cateTrans.isEmpty){
      item.replace(item.split(",")(0), cateTrans.map(_(1)).apply(0))  
    }else {
      item
    }
  }
  
  def selectionitem(x:List[String]):List[String]={
   if(x.size > 16){
    List(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),
                  x(11),x(13),if(x(20)==" ") "$#*" else x(20))
    }else {
      List(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),
                  x(11),x(12),if(x(13)==" ") "$#*" else x(13))
    }
  }
  
  def raw_split(raw_data:Array[String]):Array[String] = {
    val fi0 = raw_data(0) + "," + raw_data(1).split("\",").head.replace(",","") + "," + raw_data(1).split("\",").reverse.head + "," + raw_data(2).replace("\"","") + "," + raw_data(3).replace("\"","")

    val fi4 = raw_data(4).split("\",")

    val fi4_1 = fi4(0).replace(",","")

    val fi4_2 = fi4(1)
    
    val test = raw_data(5)

    val fi5 = raw_data(5).replace("\",","*%$#").replace(",","").replace("*%$#",",")

    val fi = (fi0 + "," + fi4_1 + "," + fi4_2 + "," + fi5).split(",")
 println(fi.size)
    if(fi.size ==21 || fi.size == 20){
      if(fi(18).size == 10){
        return fi(18).substring(0,7).replace("-","14") +: fi
      }else {
        return Array[String]()
      }
    }else {
      return Array[String]()
    }
  }
  def parseStrTONum(str:String):Double={
   var num = 0.0 
   val pattern = "^(([0-9]+.[0-9]*[1-9][0-9]*)|([0-9]*[1-9][0-9]*.[0-9]+)|([0-9]*[1-9][0-9]*))$".r
   if(str != null && str !=""){
     val numbers = pattern.findAllIn(str).toList.mkString("")
     if(numbers == str){
       num = numbers.toDouble
     }
   }
   return num
  }
  
  def divide(str1:String,str2:String):String={
    if(parseStrTONum(str2)!=0){
      return (parseStrTONum(str1)/parseStrTONum(str2)).toString()
    }else{
      return (parseStrTONum(str1)/1).toString()
    }
  }
  
  def PacksizeCoding(itemdesc: String, packname: String, packlist: List[Float]): List[Float] = {
    if (itemdesc.indexOf(packname) >= 0) {
      val packpos = itemdesc.indexOf(packname)
      val leftString = itemdesc.dropRight(itemdesc.size - packpos)
      val c = List()
      var rightPack = 1.toFloat
      val leftPack = toLeftCoding(leftString, c)
      val rightString = itemdesc.drop(packpos + packname.length())
      val result = PacksizeCoding(rightString, packname, packlist)
      if (!rightString.isEmpty()) {
        if (rightString.apply(0) == '*' || rightString.apply(0).toUpper == 'X' ) {
          rightPack = toRightCoding(rightString.drop(1), c)
        } else { rightPack = 1 }
        ((leftPack * rightPack) :: packlist) ++ result
      } else { (leftPack :: packlist) ++ result }
    } else List()
  }

  def toLeftCoding(x: String, y: List[Char]): Float = {
    var num = 0.toFloat
    try {
      num = y.mkString.toFloat
    } catch {
      case e: NumberFormatException => num = 0.toFloat
    }

    if (!x.isEmpty()) {
      if (x.reverse.head.isDigit) {
        toLeftCoding(x.reverse.drop(1).reverse, DSC2BSC(x.reverse.head) :: y)
      } else if (x.reverse.head == '.' && !y.isEmpty) {
        toLeftCoding(x.reverse.drop(1).reverse, DSC2BSC(x.reverse.head) :: y)
      } else if ((x.reverse.head == '*' || x.reverse.head.toUpper == 'X') && !y.isEmpty) {
        if (toLeftCoding(x.reverse.drop(1).reverse, List()) != 0) {
          toLeftCoding(x.reverse.drop(1).reverse, List()) * num
        } else {
          num
        }
      } else if (x.reverse.head == '+' && !y.isEmpty) {
        if (toLeftCoding(x.reverse.drop(1).reverse, List()) != 0) {
          toLeftCoding(x.reverse.drop(1).reverse, List()) + num
        } else {
          num
        }
      } else if (!y.isEmpty) {
        num
      } else 0
    } else 0
  }

  def toRightCoding(x: String, y: List[Char]): Float = {
    var temp =  x.trim()
    var num = 0.toFloat
    try {
      num = y.mkString.toFloat
    } catch {
      case e: NumberFormatException => num = 0.toFloat
    }
    if (!temp.isEmpty()) {
      if (temp.head.isDigit && !temp.drop(1).isEmpty()) {
        toRightCoding(temp.drop(1), (DSC2BSC(temp.head) :: y.reverse).reverse)
      } else if (temp.head == '.' && !temp.drop(1).isEmpty()) {
        toRightCoding(temp.drop(1), (DSC2BSC(temp.head) :: y.reverse).reverse)
      } else if (temp.head.isDigit && temp.drop(1).isEmpty()) {
        (DSC2BSC(temp.head) :: y.reverse).reverse.mkString.toFloat
      } else if (!y.isEmpty) {
        num
      } else 1
    } else 1
  }

 
  private def DSC2BSC(input: Char): Char =
    {
      var i = input;
      if (i == '\u3000') {
        i = ' ';
      } else if (i > '\uFF00' && i < '\uFF5F') {
        i = (i - 65248).toChar;
      }
      return i;

    }
  
  def packsizetransform(input:(String, String)):(String, String) = {
      var num = input._1.toFloat
      var unit = replaceC2E(input._2)
      if(unit == "KG"){
        num = 1000 * num
        unit = "G"
      }else if(unit == "L"){
        num = 1000 * num
        unit = "ML"
      }else if(unit == "OZ"){
        num = 28.3495231.toFloat * num
        unit = "G"
      }else if(unit == "J"){
        num = 500 * num
        unit = "G"
      }
      
      return (num.toString, unit)
    }
  
 def replaceC2E(CS: String): String = {
    if (CS == "克") {
      "G"
    } else if (CS == "千克") {
      "KG"
    } else if (CS == "毫升") {
      "ML"
    } else if (CS == "升") {
      "L"
    } else if (CS == "公升") {
      "L"
    } else if (CS == "公斤") {
      "KG"
    } else if (CS == "盎司") {
      "OZ"
    } else if (CS == "片") {
      "P"
    } else if (CS == "斤") {
      "J"
    } else CS

  }
 
}