package com.pharbers.process.stm.step.pptx.slider.content

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.poi.xslf.usermodel.XSLFSlide
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

trait phReportContent {
    var jobid = ""
    var data: Any = _
    var slideIndex = 0
    var text: JsValue = _
    var tables: List[JsValue] = Nil

    def init(args: Any): Unit ={
        val argMap = args.asInstanceOf[Map[String, Any]]
        jobid = argMap("jobid").asInstanceOf[String]
        data = argMap("data")
        slideIndex = argMap("slideIndex").asInstanceOf[Int]
        val content = argMap("content").asInstanceOf[JsValue]
        text = (content \ "texts").as[JsValue]
        tables = (content \ "tables").as[List[JsValue]]
    }

    def setElementInSlider(className: String, element: Any, data: Any = null, slideIndex: Int, jobid: String = ""): Unit ={
        phLyFactory.getInstance(className).asInstanceOf[phCommand]
            .exec(Map("element" -> element, "data" -> data, "slideIndex" -> slideIndex, "jobid" -> jobid))
    }

}

class phReportContentImpl extends phReportContent with phCommand {
    override def exec(args: Any): Any = {
        init(args)
        tables.foreach(table => {
            val factory = (table \ "factory").as[String]
            setElementInSlider(factory, table, data, slideIndex, jobid)
        })
        setElementInSlider("com.pharbers.process.stm.step.pptx.slider.content.phReportContentTextImpl", text, data, slideIndex = slideIndex, jobid = jobid)
    }
}

class phReportMuchDataContentImpl extends phReportContent with phCommand {
    override def exec(args: Any): Any = {
        init(args)
        tables.foreach(table => {
            val factory = (table \ "factory").as[String]
            val tableData = data.asInstanceOf[Map[String, Any]]((table \ "data").as[String])
            setElementInSlider(factory, table, tableData, slideIndex, jobid)
        })
        setElementInSlider("com.pharbers.process.stm.step.pptx.slider.content.phReportContentTextImpl", text, data, slideIndex = slideIndex, jobid = jobid)
    }
}
