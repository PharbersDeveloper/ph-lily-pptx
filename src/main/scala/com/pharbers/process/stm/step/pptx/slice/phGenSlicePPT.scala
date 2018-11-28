package com.pharbers.process.stm.step.pptx.slice

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

trait phGenSlicePPT {
    var df : Option[DataFrame] = None
    def genSliceDataFrame(filter : JsValue) : Unit = {
        // TODO: 从匹配表中的信息，形成一个Filter，将主表Filter出来保存到df中
        val factory = (filter \ "factory").asOpt[String].get
        df = Some(phLyFactory.getInstance(factory).asInstanceOf[phCommand].exec().asInstanceOf[DataFrame])
    }
}

class phGenSlicePPTImpl extends phGenSlicePPT with phCommand {
    override def exec(args: Any): Any = {
        val format = args.asInstanceOf[JsValue]
        val filter = (format \ "filter").asOpt[JsValue].get
        genSliceDataFrame(filter)

        val sliders = (format \ "sliders").asOpt[List[JsValue]].get
        sliders.map { iter =>
            phLyFactory.getInstance("com.pharbers.process.stm.step.pptx.slider.phGenSliderImpl").
                    asInstanceOf[phCommand].exec(
                Map(
                    "data" -> this.df,
                    "slider" -> iter
                )
            )
        }
    }
}
