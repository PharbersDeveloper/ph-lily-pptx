package com.pharbers.process.stm.step.pptx.slider.content

import com.pharbers.phsocket.phSocketDriver
import com.pharbers.process.common.jsonData.phText
import com.pharbers.process.common.{phCommand, phLyFactory}
import play.api.libs.json.JsValue
import play.api.libs.json._
import play.api.libs.functional.syntax._

trait phReportContentText {
    val socketDriver = phSocketDriver()
    implicit val textFormat: Format[phText] = Json.format[phText]
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

            (x \ "match").as[List[JsValue]].foreach(x => {
                val textMode = x.as[phText]
                val data = argMap("data").asInstanceOf[Map[String, Any]](textMode.data)
                val contentMap = phLyFactory.getInstance(textMode.factory).asInstanceOf[phCommand].exec(Map(
                    "data" ->data, "name" -> textMode.name, "colList" -> textMode.colList, "timeline" -> textMode.timeline,
                    "displayNameList" -> textMode.displayNameList, "primaryValueName" -> textMode.primaryValueName
                )).asInstanceOf[Map[String, Double]]
                textMode.name.foreach(x => {
                    content = content.replace(s"#$x#", contentMap(x).toString)
                })
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