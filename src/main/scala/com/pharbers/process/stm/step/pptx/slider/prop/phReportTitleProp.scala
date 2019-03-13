package com.pharbers.process.stm.step.pptx.slider.prop

import java.awt.Rectangle

import com.pharbers.phsocket.phSocketDriver
import com.pharbers.process.common.phCommand
import org.apache.poi.xslf.usermodel.XSLFSlide

trait phReportTitleProp extends phReportProp {
    val socketDriver = phSocketDriver()
}

class phReportTitlePropImpl extends phReportTitleProp with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        val jobid = argMap("jobid").asInstanceOf[String]
        val slideIndex = argMap("slideIndex").asInstanceOf[Int]
        val pos = List(35,35,2286,158).map(x => (x / 0.000278).toInt)
        val contentString = argMap("title").asInstanceOf[String]
        val content = s"#{##[#$contentString#C#default#]##P#center#}#"
        val css = "test"
        socketDriver.createTitle(jobid,content,pos,slideIndex,css)
    }
}
