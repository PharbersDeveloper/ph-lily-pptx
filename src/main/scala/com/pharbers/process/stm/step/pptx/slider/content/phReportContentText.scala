package com.pharbers.process.stm.step.pptx.slider.content

import com.pharbers.phsocket.phSocketDriver
import com.pharbers.process.common.{phCommand, phLyFactory}
import com.pharbers.process.stm.step.pptx.slider.content.phContentText.overview.prodSomInChpa
import play.api.libs.json.JsValue

trait phReportContentText {
    val socketDriver = phSocketDriver()
}

class phReportContentTextImpl extends phReportContentText with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        val jobid = argMap("jobid").asInstanceOf[String]
        val slideIndex = argMap("slideIndex").asInstanceOf[Int]

        argMap("element").asInstanceOf[List[JsValue]].foreach(x => {
            var content = (x \ "text").as[List[JsValue]].map(x => {
                val runs = (x \ "runs").as[List[JsValue]].map(r => {
                    phLyFactory.getInstance((r \ "factory").as[String]).asInstanceOf[phCommand].exec(Map("element" -> r, "data" -> argMap("data"))).asInstanceOf[String]
                }).mkString("")
                "#{#" + runs + "#P#" + (x \ "format").as[String] + "#}#"
            }).mkString("")

            val data = argMap("data").asInstanceOf[Map[String, Any]]((x \ "match" \ "data").as[String])
            val contentMap = phLyFactory.getInstance((x \ "match" \ "factory").as[String]).asInstanceOf[phCommand].exec(
                Map("name" -> (x \ "match" \ "name").as[List[String]], "data" -> data, "timeline" -> phReportContentTable.time2timeLine((x \ "match" \ "timeline").as[String]))
            ).asInstanceOf[Map[String, String]]
            (x \ "match" \ "name").as[List[String]].foreach(x => {
                content = content.replace(s"#$x#", contentMap(x))
            })
            socketDriver.createTitle(jobid, content, (x \ "pos").as[List[Int]].map(x => (x / 0.000278).toInt), slideIndex, "")
        })
    }
}

class PhNormalRunText extends phCommand {
    override def exec(args: Any): Any = {
        val run = args.asInstanceOf[Map[String, Any]]("element").asInstanceOf[JsValue]
        "#[#" + (run \ "text").as[String] + "#C#" + (run \ "format").as[String] + "#]#"
    }
}

class PhDateRunText extends phCommand {
    override def exec(args: Any): Any = {
        val run = args.asInstanceOf[Map[String, Any]]("element").asInstanceOf[JsValue]
        "#[#" + phReportContentTable.time2timeLine((run \ "text").as[String], (run \ "timeFormat").as[String]) + "#C#" + (run \ "format").as[String] + "#]#"
    }
}