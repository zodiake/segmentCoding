package com.nielsen.dataformat

/**
  * @author daiyu01
  */
trait CsvFormat[A] {
  def toCsvString(config: A): String
}

case class SuNingRaw(storeId: String,
                     city: String,
                     itemCode: String,
                     prodCate: String,
                     EAN: String,
                     desc: String,
                     salesValue: Double,
                     salesVolumn: Double,
                     payValue: Double,
                     pay: String
                    ) extends CsvFormat[String] {

  def toCsvString(year: String): String = {
    val storeCode = if (pay != "0") "20270" else "20271"
    val unitPrice = if (salesVolumn == 0) 0 else salesValue * 10000 / salesVolumn
    //storeId + "," + "\"" + city + "\"" + "," + itemCode + "," + storeCode + "," + "SUNING" + "," + prodCate.replace(",", "") + "," + EAN + "," + "" + "," + "" + "," + "\"" + "\"" + "," + "\"" + "\"" + "," + "" + "," + "\"" + desc.replace(",", "") + "\"" + "," + salesValue * 10000 + "," + 1 + "," + salesVolumn + "," + payValue * 10000 + "," + payValue * 10000 + "," + year.substring(0, 4) + "-" + year.substring(6) + "-" + "01" + "," + "\"" + " " + "\"" + "," + ""
    val newFormat =
      s"""${storeId},"${city}",${itemCode},${storeCode},SUNING,${prodCate.replace(",", "")},${EAN},,,"","",,"${desc.replace(",", "")}",${unitPrice},1,${salesVolumn},${payValue * 10000},${unitPrice},${year.substring(0, 4)}-${year.substring(6)}-01," ", """
    //val oldFormat = storeId + "," + "\"" + city + "\"" + "," + itemCode + "," + storeCode + "," + "SUNING" + "," + prodCate.replace(",", "") + "," + EAN + "," + "" + "," + "" + "," + "\"" + "\"" + "," + "\"" + "\"" + "," + "" + "," + "\"" + desc.replace(",", "") + "\"" + "," + salesValue * 10000 + "," + 1 + "," + salesVolumn + "," + payValue * 10000 + "," + payValue * 10000 + "," + year.substring(0, 4) + "-" + year.substring(6) + "-" + "01" + "," + "\"" + " " + "\"" + "," + ""
    newFormat
  }

}

