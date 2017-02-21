package com.nielsen.dataformat

/**
  * @author daiyu01
  */
case class SuNingRaw(
                      val storeId: String,
                      val city: String,
                      val itemCode: String,
                      val prodCate: String,
                      val EAN: String,
                      val desc: String,
                      val salesValue: Double,
                      val salesVolumn: Double,
                      val payValue: Double,
                      val pay: String
                    ) {

  def toCsvString(year: String): String = {
    val storeCode = if (pay != "0") "20270" else "20271"
    storeId + "," + "\"" + city + "\"" + "," + itemCode + "," + storeCode + "," + "SUNING" + "," + prodCate.replace(",", "") + "," + EAN + "," + "" + "," + "" + "," + "\"" + "\"" + "," + "\"" + "\"" + "," + "" + "," + "\"" + desc.replace(",", "") + "\"" + "," + salesValue * 10000 + "," + 1 + "," + salesVolumn + "," + payValue * 10000 + "," + payValue * 10000 + "," + year.substring(0, 4) + "-" + year.substring(6) + "-" + "01" + "," + "\"" + " " + "\"" + "," + ""
    //s"""${x.storeId},"${x.city}",${x.itemCode},${storeCode},SUNING,${x.prodCate.replace(",","")}, ${x.EAN},,,"","",,"${x.desc.replace(",","")}",${x.salesValue*10000},1,${x.salesVolumn},${x.payValue*10000},${x.payValue*10000},${year.substring(0,4)}-${year.substring(6)}-01," ", """
  }

}
  
