package com.pharbers.process.stm.step.pptx.slider.content

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.poi.xslf.usermodel.XSLFSlide
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

trait phReportContent {
    var slide: XSLFSlide = _
    def setElementInSlider(className: String, element: Any, data: Any = null): Unit ={
        phLyFactory.getInstance(className).asInstanceOf[phCommand]
            .exec(Map("ppt_inc" -> slide, "element" -> element, "data" -> data))
    }

}

class phReportContentImpl extends phReportContent with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        slide = argMap("ppt_inc").asInstanceOf[XSLFSlide]
        val data = argMap("data").asInstanceOf[DataFrame]
        val content = argMap("content").asInstanceOf[JsValue]
        val text = (content \ "texts").as[JsValue]
        val table = (content \ "tables").as[JsValue]
        setElementInSlider("com.pharbers.process.stm.step.pptx.slider.content.phReportContentTextImpl", text)
        setElementInSlider("com.pharbers.process.stm.step.pptx.slider.content.phReportContentTableImpl", table, data)
    }
}
