package com.pharbers.process.stm.step.pptx.filter

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import play.api.libs.json.JsValue

trait phFilter {
    val formatYm = udf((data: Any) => {
        val ymlst = data.toString.split("/")
        val year: String = ymlst.last
        val month: String = ymlst.head
        val result = year + month
        result
    })
}

class phSearchFilterImpl extends phFilter with phCommand {
    override def exec(args: Any): DataFrame = {
        val js = args.asInstanceOf[JsValue]
        val name = (js \ "name").as[String]
        val source = phLyFactory.getStorageWithDFName("DF_gen_search_set").filter(col("name") === name)
            .withColumn("DATE", formatYm(col("DATE")))
        source
    }
}

class phMOVFilterImpl extends phFilter with phCommand {
    override def exec(args: Any): Any = {
        val js = args.asInstanceOf[JsValue]
        val name = (js \ "name").as[List[String]]
        val movSourceList = List("LLYProd", "Manufa", "ManufaMNC", "market", "DF_gen_search_set", "DF_gen_city_search_set")
        val getDF: String => DataFrame = str => {
            phLyFactory.getStorageWithDFName(str)
        }
        val result = name.map(x => (x,
                if (movSourceList.contains(x)) getDF(x).withColumn("DATE", formatYm(col("DATE")))
                else getDF(x)
        )).toMap
        result
    }
}

class phCityFilterImpl extends phFilter with phCommand {
    override def exec(args: Any): DataFrame = {
        val js = args.asInstanceOf[JsValue]
        val name = (js \ "name").as[String]
        val cityFilt = (js \ "filt").as[List[String]]
        val source = phLyFactory.getStorageWithDFName("DF_gen_city_search_set").filter(col("name") === name)
            .filter(col("CITY").isin(cityFilt: _*))
            .withColumn("DATE", formatYm(col("DATE")))
        source.select("CITY", "PRODUCT NAME", "PACK DES", "DATE", "TYPE", "ADD RATE", "DOT", "VALUE", "Display Name")
    }
}

class phGlpFilterImpl extends phFilter with phCommand {
    override def exec(args: Any): Any = {
        val js = args.asInstanceOf[JsValue]
        val name = (js \ "name").as[String]
        val countryDataName = (js \ "countryDataName").as[String]
        val shareDataName = (js \ "shareDataName").as[String]
        val countryData = phLyFactory.getStorageWithDFName(countryDataName).filter(col("name") === name)
            .withColumn("DATE", formatYm(col("DATE")))
        val shareData = phLyFactory.getStorageWithDFName(shareDataName).select("TC_II", "TC_IV","DATE", "TYPE", "VALUE", "ADD RATE", "DOT", "CITY")
            .withColumn("DATE", formatYm(col("DATE")))
        Map(countryDataName -> countryData, shareDataName -> shareData)
    }
}

class phGlpCityFilterImpl extends phFilter with phCommand {
    override def exec(args: Any): Any = {
        val js = args.asInstanceOf[JsValue]
        val name = (js \ "name").as[String]
        val cityFilt = (js \ "filt").as[List[String]].head
        val countryDataName = (js \ "countryDataName").as[String]
        val shareDataName = (js \ "shareDataName").as[String]
        val countryData = phLyFactory.getStorageWithDFName(countryDataName).filter(col("CITY") === cityFilt)
            .filter(col("name") === name)
            .withColumn("DATE", formatYm(col("DATE")))
        val shareData = phLyFactory.getStorageWithDFName(shareDataName).filter(col("CITY") === cityFilt)
            .select("TC_II", "TC_IV","DATE", "TYPE", "VALUE", "ADD RATE", "DOT", "CITY")
            .withColumn("DATE", formatYm(col("DATE")))
        Map(countryDataName -> countryData, shareDataName -> shareData)
    }
}