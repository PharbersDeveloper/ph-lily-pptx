package com.pharbers.process.stm.step.pptx.slider.content

import java.io.{File, PrintWriter}

import com.pharbers.phsocket.phSocketDriver
import com.pharbers.process.common.jsonData.phText
import com.pharbers.process.common.{phCommand, phLyFactory}
import com.pharbers.process.stm.step.pptx.slider.content.phContentText.overview.prodSomInChpa
import play.api.libs.json.JsValue
import play.api.libs.json._
import play.api.libs.functional.syntax._

import scala.io.Source

trait phReportContentText {
    val socketDriver = phSocketDriver()
    implicit val textFormat: Format[phText] = Json.format[phText]
}

class phReportContentTextImpl extends phReportContentText with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        val jobid = argMap("jobid").asInstanceOf[String]
        val slideIndex = argMap("slideIndex").asInstanceOf[Int]

        argMap("element").asInstanceOf[List[JsValue]].map(x => {
            var index = 0
            var content = (x \ "text").as[List[JsValue]].map(x => {
                val runs = (x \ "runs").as[List[JsValue]].map(r => {
                    index += 1
                    phLyFactory.getInstance((r \ "factory").as[String]).asInstanceOf[phCommand].exec(Map("element" -> r, "data" -> argMap("data"), "index" -> index)).asInstanceOf[String]
                }).mkString("")
                "#{#" + runs + "#P#" + (x \ "format").as[String] + "#}#"
            }).mkString("")

            (x \ "match").as[List[JsValue]].foreach(x => {
                val textMode = x.as[phText]
                var data: Any = null
                if (textMode.data == ""){
                    data = argMap("data")
                } else {
                    data = argMap("data").asInstanceOf[Map[String, Any]](textMode.data)
                }
                val contentMap = phLyFactory.getInstance(textMode.factory).asInstanceOf[phCommand].exec(Map(
                    "data" -> data, "name" -> textMode.name, "colList" -> textMode.colList, "timeline" -> phReportContentTable.time2timeLine(textMode.timeline),
                    "allDisplayNames" -> textMode.displayNameList, "primaryValueName" -> textMode.primaryValueName
                )).asInstanceOf[Map[String, Double]]
                textMode.name.foreach(x => {
                    content = content.replace(s"#$x#", (contentMap(x) / textMode.unit(textMode.name.indexOf(x))).formatted("%.2f").toString)
                })
            })

//            val shapeJson = Json.obj("type" -> "txt", "pos" -> (x \ "pos").as[List[Int]].map(x => (x / 0.000278).toInt), "format" -> content , "shapeType" -> (x \ "shapeType").asOpt[String].getOrElse("Rectangle"))
            socketDriver.createText(jobid, content, (x \ "pos").as[List[Int]].map(x => (x / 0.000278).toInt), slideIndex, (x \ "shapeType").asOpt[String].getOrElse("Rectangle"))
//            shapeJson
        })
    }
}

class PhNormalRunText extends phCommand {
    override def exec(args: Any): Any = {
        val run = args.asInstanceOf[Map[String, Any]]("element").asInstanceOf[JsValue]
        val index = args.asInstanceOf[Map[String, Any]]("index").asInstanceOf[Int]
        if(index == 1) {
            "#[#" + "txt" + "#C#" + (run \ "format").as[String] + "#]#"
        } else {
            "#[#" + "txt" + index + "#C#" + (run \ "format").as[String] + "#]#"
        }
    }
}

class PhNormalRunTextForMongo extends phCommand {
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