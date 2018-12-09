package com.pharbers.process.stm.step.pptx.slider.content

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.poi.xslf.usermodel.XSLFSlide
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

trait phReportContent {
    var slide: XSLFSlide = _
    def setElementInSlider(className: String, element: Any, data: Any = null, slideIndex: Int, jobid: String = ""): Unit ={
        phLyFactory.getInstance(className).asInstanceOf[phCommand]
            .exec(Map("ppt_inc" -> slide, "element" -> element, "data" -> data, "slideIndex" -> slideIndex, "jobid" -> jobid))
    }

}

class phReportContentImpl extends phReportContent with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        slide = argMap("ppt_inc").asInstanceOf[XSLFSlide]
        val jobid = argMap("jobid").asInstanceOf[String]
        val data = argMap("data").asInstanceOf[DataFrame]
        val slideIndex = argMap("slideIndex").asInstanceOf[Int]
        val content = argMap("content").asInstanceOf[JsValue]
        val text = (content \ "texts").as[JsValue]
        val tables = (content \ "tables").as[List[JsValue]]
        tables.foreach(table => {
            val factory = (table \ "factory").as[String]
            setElementInSlider(factory, table, data, slideIndex, jobid)
        })
        setElementInSlider("com.pharbers.process.stm.step.pptx.slider.content.phReportContentTextImpl", text, slideIndex = slideIndex, jobid = jobid)
    }
}

//class phReportContentForTrends extends phReportContent with phCommand {
//    override def exec(args: Any): Any = {
//        val argMap = args.asInstanceOf[Map[String, Any]]
//        slide = argMap("ppt_inc").asInstanceOf[XSLFSlide]
//        val data = argMap("data").asInstanceOf[DataFrame]
//        val content = argMap("content").asInstanceOf[JsValue]
//        val text = (content \ "texts").as[JsValue]
//        val table = (content \ "tables").as[JsValue]
//        val mktDisplayName = (content \ "mkt_display").as[String]
//        setElementInSlider("com.pharbers.process.stm.step.pptx.slider.content.phReportContentTextImpl", text)
//        phLyFactory.getInstance("com.pharbers.process.stm.step.pptx.slider.content.phReportContentTrendsTable").asInstanceOf[phCommand]
//                .exec(Map("ppt_inc" -> slide, "element" -> table, "data" -> data, "mkt_display" -> mktDisplayName))
//    }
//}
