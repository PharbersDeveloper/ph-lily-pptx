package com.pharbers.process.stm.step.pptx.slice

import java.net.Socket

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

trait phGenSlicePPT {
    var df : Any = _
    def genSliceDataFrame(filter : JsValue) : Unit = {
        // 从匹配表中的信息，形成一个Filter，将主表Filter出来保存到df中
        val factory = (filter \ "factory").asOpt[String].get
        df = phLyFactory.getInstance(factory).asInstanceOf[phCommand].exec(filter)
    }
}

class phGenSlicePPTImpl extends phGenSlicePPT with phCommand {
    override def exec(args: Any): Int = {
        val tmp = args.asInstanceOf[Map[String, Any]]
        val jobid = tmp("jobid").asInstanceOf[String]
        val format = tmp("slider").asInstanceOf[JsValue]
        var slideIndex = tmp("slideIndex").asInstanceOf[Int]
        val filter = (format \ "filter").asOpt[JsValue].get
        genSliceDataFrame(filter)

        val sliders = (format \ "sliders").asOpt[List[JsValue]].get
        sliders.foreach { iter =>
            slideIndex += 1
            phLyFactory.getInstance("com.pharbers.process.stm.step.pptx.slider.phGenSliderImpl").
                    asInstanceOf[phCommand].exec(
                Map(
                    "data" -> this.df,
                    "slider" -> iter,
                    "ppt" -> tmp("ppt"),
                    "slideIndex" -> slideIndex,
                    "jobid" -> jobid
                )
            )
        }
        slideIndex
    }
}
