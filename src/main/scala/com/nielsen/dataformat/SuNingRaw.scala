package com.nielsen.dataformat

/**
  * @author daiyu01
  */
class SuNingRaw(
                 val storeId: String,
                 val city: String,
                 val itemCode: String,
                 val prodCate: String,
                 val EAN: String,
                 val desc: String,
                 val salesValue: Double,
                 val salesVolumn: Double,
                 val payValue: Double,
                 val pay: Double
               ) extends Serializable
  
