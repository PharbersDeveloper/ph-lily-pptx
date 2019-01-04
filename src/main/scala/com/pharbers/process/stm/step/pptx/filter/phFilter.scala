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
        val movSourceList = List("LLYProd", "Manufa", "ManufaMNC", "market")
        val getDF: String => DataFrame = str => {
            phLyFactory.getStorageWithDFName(str)
    }
        name.map(x => (x,
            if (x.contains(movSourceList)) getDF(x).withColumn("DATE", formatYm(col("DATE")))
            else getDF(x)
        )).toMap
    }
}
