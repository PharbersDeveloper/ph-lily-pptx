package com.pharbers.process.stm.step.pptx.slider

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

trait phGenSlider {

}

class phGenSliderImpl extends phGenSlider with phCommand {
    override def exec(args: Any): Any = {
        // TODO: 在这里获得传递进来的ppt实例，在这里创建一个slider，并添加到ppt实例中

        val tmp = args.asInstanceOf[Map[String, AnyRef]]
        val format = tmp.get("slider").get.asInstanceOf[JsValue]
        val title = (format \ "title").asOpt[String].get
        phLyFactory.getInstance("com.pharbers.process.stm.step.pptx.slider.prop.phReportTitlePropImpl").
            asInstanceOf[phCommand].exec(
                Map(
//                    "ppt_inc" -> // TODO: 你创建的ppt slider实例
                    "title" -> title
                )
            )

        val data = tmp.get("data").get.asInstanceOf[DataFrame]
        // TODO: 在这里获得传递进来的ppt实例，在这里创建一个slider，并添加到ppt实例中

//        println(data)
        data.show(false)
        println(title)
    }
}
