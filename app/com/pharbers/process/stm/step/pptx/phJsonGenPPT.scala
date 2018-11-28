package com.pharbers.process.stm.step.pptx

import com.pharbers.process.common.{phCommand, phLyFactory}
import play.api.libs.json.JsValue

trait phJsonGenPPT extends phCommand {
    override def exec(args: Any): Any = {
        val format = args.asInstanceOf[JsValue]
        val out_put_filename = (format \ "title").asOpt[String].get

        // TODO: 在这里创建PPT实例，把这个实例当作参数像里面传递
        val sl = (format \ "slice").asOpt[JsValue].get
        phLyFactory.getInstance("com.pharbers.process.stm.step.pptx.slice.phGenSlicePPTImpl").asInstanceOf[phCommand].exec(sl)

        println(out_put_filename)
    }
}