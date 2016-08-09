

object sheet1 {
  val str = List("20161405,1129326215,广东广州,17864858961,B,洗护清洁剂/卫生巾/纸/香薰,卷筒纸,,,卷筒纸,Breeze/清风,,【天猫超市】清风 马蹄莲3层100g*10卷无芯卷纸平纹长卷卫生纸巾, ,12.9,20161405100200000109662")
  val descFormat = str.map { x => (x, x.split(",", -1)) }.filter(_._2.length >= 20)
    .map { x => (x._1, x._2(4) + "," + x._2(5) + "," + x._2(6), x._1.indexOf(x._2(4) + "," + x._2(5) + "," + x._2(6))) }
    .map { x => (x._1.substring(0, x._3), 1, x._1.substring(x._3 + x._2.length())) }
    .map(x => x._1 + x._2 + x._3)
  str.head.split(",", -1).size

}