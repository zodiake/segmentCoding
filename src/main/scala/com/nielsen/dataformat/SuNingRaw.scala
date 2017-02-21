package com.nielsen.dataformat

/**
  * @author daiyu01
  */
trait csvFormat {
  def toCsvString(year: String): String
}

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
                    ) extends csvFormat {

  def toCsvString(year: String): String = {
    val storeCode = if (pay != "0") "20270" else "20271"
    val unitPrice = if (salesVolumn == 0) 0 else salesValue * 10000 / salesVolumn
    val newFormat =
      s"""${storeId},"${city}",${itemCode},${storeCode},SUNING,${prodCate.replace(",", "")},${EAN},,,"","",,"${desc.replace(",", "")}",${unitPrice},1,${salesVolumn},${payValue * 10000},${unitPrice},${year.substring(0, 4)}-${year.substring(6)}-01," ", """
    newFormat
  }

}
  
