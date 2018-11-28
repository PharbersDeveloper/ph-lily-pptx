package com.pharbers.process.stm.step.pptx.slider

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.poi.xslf.usermodel.{SlideLayout, XMLSlideShow}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

trait phGenSlider {

}

class phGenSliderImpl extends phGenSlider with phCommand {
    override def exec(args: Any): Any = {
        // TODO: 在这里获得传递进来的ppt实例，在这里创建一个slider，并添加到ppt实例中
        val tmp = args.asInstanceOf[Map[String, AnyRef]]
        val ppt = tmp("ppt").asInstanceOf[XMLSlideShow]
        val master = ppt.getSlideMasters.get(0)
        val slider = ppt.createSlide(master.getLayout(SlideLayout.TITLE))
        val format = tmp.get("slider").get.asInstanceOf[JsValue]
        val title = (format \ "title").asOpt[String].get
        phLyFactory.getInstance("com.pharbers.process.stm.step.pptx.slider.prop.phReportTitlePropImpl").
                asInstanceOf[phCommand].exec(
            Map(
                "ppt_inc" -> slider, // TODO: 你创建的ppt slider实例
                "title" -> title
            )
        )

        val data = tmp.get("data").get.asInstanceOf[DataFrame]
        // TODO: 在这里获得传递进来的ppt实例，在这里创建一个slider，并添加到ppt实例中
        val content = (format \ "content").asOpt[JsValue].get
        phLyFactory.getInstance("com.pharbers.process.stm.step.pptx.slider.content.phReportContentImpl").
                asInstanceOf[phCommand].exec(
            Map(
                "ppt_inc" -> slider,
                "content" -> content,
                "data" -> data
            )
        )
        //        println(data)
        //        data.show(false)
        println(title)
    }
}
