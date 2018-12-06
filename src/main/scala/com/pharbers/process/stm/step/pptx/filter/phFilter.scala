package com.pharbers.process.stm.step.pptx.filter

import com.pharbers.process.common.{phCommand, phLyFactory}
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

trait phFilter{

}

class phSearchFilterImpl extends phFilter with phCommand {
    override def exec(args: Any): DataFrame = {
        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        import org.apache.spark.sql.functions._

        val js = args.asInstanceOf[JsValue]
        val name = (js \ "name").as[String]
        val source = phLyFactory.getStorageWithDFName("DF_gen_search_set").filter(col("name") === name)
        source
    }
}
