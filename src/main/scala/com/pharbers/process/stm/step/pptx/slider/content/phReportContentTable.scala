package com.pharbers.process.stm.step.pptx.slider.content

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.poi.xslf.usermodel.XSLFSlide
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

trait phReportContentTable {
    var slide: XSLFSlide = _
}

class phReportContentTableImpl extends phReportContentTable with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        slide = argMap("ppt_inc").asInstanceOf[XSLFSlide]
        val data = argMap("data").asInstanceOf[DataFrame]
        argMap("element").asInstanceOf[JsValue].as[List[JsValue]].foreach(x => {
            val pos = (x \ "pos").as[List[Int]]
            val timeline = (x \"timeline").as[List[String]]
            val col = (x \ "col").as[List[String]]
            val row = (x \ "row").as[List[String]]

            val table = slide.createTable()
            val function = "com.pharbers.process.stm.step.pptx.slider.content." + col(0)
            val value = phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
                Map("data" -> data, "displayName" -> row(0), "ym" -> timeline(0))
            )
            println(value)
        })
    }
}