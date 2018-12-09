package com.pharbers.process.stm.step.pptx

import java.net.Socket

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.poi.xslf.usermodel.XMLSlideShow
import play.api.libs.json.JsValue

class phJsonGenPPT extends phCommand {
    override def exec(args: Any): String = {
        val tmp = args.asInstanceOf[Map[String, Any]]
        val jobid = tmp("jobid").asInstanceOf[String]
        val format = tmp("format").asInstanceOf[JsValue]
        val out_put_filename = (format \ "title").asOpt[String].get
        // TODO: 在这里创建PPT实例，把这个实例当作参数像里面传递
        val ppt = new XMLSlideShow()
        phLyFactory.stssoo += ("ppt" -> ppt)
        var slideIndex = -1
        (format \ "slices").asOpt[List[JsValue]].get.foreach{sl =>
            slideIndex += 1
            phLyFactory.getInstance("com.pharbers.process.stm.step.pptx.slice.phGenSlicePPTImpl").asInstanceOf[phCommand].exec(
                Map(("slider", sl), ("ppt", ppt), ("slideIndex", slideIndex), ("jobid", jobid))
            )}
        println(out_put_filename)
        out_put_filename
    }
}
