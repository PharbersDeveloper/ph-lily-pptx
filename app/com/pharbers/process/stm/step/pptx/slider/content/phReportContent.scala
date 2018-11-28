package com.pharbers.process.stm.step.pptx.slider.content

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.poi.xslf.usermodel.XSLFSlide
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

trait phReportContent {

}

class phReportContentImpl extends phReportContent with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        val slide = argMap("ppt_inc").asInstanceOf[XSLFSlide]
        val data = argMap("data").asInstanceOf[DataFrame]
        val content = argMap("content").asInstanceOf[JsValue]
        val text = (content \ "texts").asOpt[JsValue]
        val table = (content \ "tables").asOpt[JsValue]
        phLyFactory.getInstance("com.pharbers.process.stm.step.pptx.slider.content.phReportContentTextImpl")
            .asInstanceOf[phCommand].exec()
    }
}
