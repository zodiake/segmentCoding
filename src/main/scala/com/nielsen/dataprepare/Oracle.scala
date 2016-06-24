package com.nielsen.dataprepare
import java.sql._


class Oracle {

	def getCONFResult():List[String] = {

		Class.forName("oracle.jdbc.driver.OracleDriver")

		val lda = DriverManager getConnection "jdbc:oracle:oci:ecom/ecom@ECCH02PR"

		val sth = lda createStatement

		val sq = sth executeQuery "select segid,a.catcode,segno,segtype,csegment,segname,shortdesc," +
								"case when b.eccmanu is null then '-' else b.eccmanu end as eccmanu," +
								"case when esegname is null then '-' else esegname end as esegname, " +
								"case when b.ecmanu is null then '-' else b.ecmanu end as ecmanu,fno " +
								"from db_dic_segment a left outer join db_cate_manu_brand b " +
								"on a.catcode = b.catcode " +
								"and a.segcode = b.ecbrandcode"

		var rs = List[String]()

		while(sq next){ 

			var row_rs = List[String]()

			for(i <- 1 to 11) {

				row_rs = sq.getString(i) :: row_rs

			}
			
			rs = row_rs.reverse.mkString(",") :: rs

		}

		return rs

	}

	

}