package com.pharbers.phsocket

import java.io.DataOutputStream
import java.util.UUID

import com.pharbers.baseModules.PharbersInjectModule
import com.pharbers.pptxmoudles._
import com.pharbers.macros._
import com.pharbers.macros.convert.jsonapi.JsonapiMacro._
import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport
import com.pharbers.moduleConfig.{ConfigDefines, ConfigImpl}
import io.circe.syntax._
import play.api.libs.json._

import scala.io.Source
import scala.xml.{Node, NodeSeq}

trait phSocket_managers extends createPPT with setExcel with excel2PPT with createTitle

sealed trait phRequest extends phSocket_trait {
    val dataOutputStream = new DataOutputStream(socket.getOutputStream)
    def sendMessage(msg: String): Unit ={
        Thread.sleep(1000)
        dataOutputStream.writeUTF(msg)
        dataOutputStream.flush()
    }
}

trait createPPT extends phRequest with CirceJsonapiSupport{
    def createPPT(jobid: String): Unit ={
        val id: String = UUID.randomUUID().toString
        val request = new PhRequest
        request.id = id
        request.`type` = "PhRequest"
        request.command = "GenPPT"
        request.jobid = jobid
        val msg = toJsonapi(request).asJson.toString()
        println(msg)
        sendMessage(msg)
    }
}

trait setExcel extends phRequest with CirceJsonapiSupport with createExcelCss{
    def setExcel(jobid: String, excelName: String, cell: String, value: String, cate: String, cssName: List[String]): Unit = {
        val id: String = UUID.randomUUID().toString
        val request = new PhRequest
        request.id = id
        request.`type` = "PhRequest"
        request.command = "ExcelPush"
        request.jobid = jobid
        val phExcelPush = new PhExcelPush
        phExcelPush.id = UUID.randomUUID().toString
        phExcelPush.`type` = "PhExcelPush"
        phExcelPush.name = excelName
        phExcelPush.cell = cell
        phExcelPush.css = Some(createCss(cssName, cell))
        phExcelPush.cate = cate    //Number or String
        phExcelPush.value = value
        request.push = Some(phExcelPush)
        val msg = toJsonapi(request).asJson.toString()
        sendMessage(msg)
    }
}

trait excel2PPT extends phRequest with CirceJsonapiSupport{
    def excel2PPT(jobid: String, excelName: String, pos: List[Int], sliderIndex: Int): Unit ={
        val id: String = UUID.randomUUID().toString
        val request = new PhRequest
        request.id = id
        request.`type` = "PhRequest"
        request.command = "Excel2PPT"
        request.jobid = jobid
        val phExcel2PPT = new PhExcel2PPT
        phExcel2PPT.id = UUID.randomUUID().toString
        phExcel2PPT.`type` = "Excel2PPT"
        phExcel2PPT.name = excelName
        phExcel2PPT.pos = pos
        phExcel2PPT.slider = sliderIndex
        request.e2p = Some(phExcel2PPT)
        val msg = toJsonapi(request).asJson.toString()
        sendMessage(msg)
    }
}

trait createTitle extends phRequest with CirceJsonapiSupport{
    def createTitle(jobid: String, content: String, pos: List[Int], slider: Int, css: String): Unit ={
        val id: String = UUID.randomUUID().toString
        val request = new PhRequest
        request.id = id
        request.`type` = "PhRequest"
        request.command = "PushText"
        request.jobid = jobid
        val phTest2PPT = new PhTextPPT
        phTest2PPT.`type` = "PhTextSetContent"
        phTest2PPT.content = content
        phTest2PPT.pos = pos
        phTest2PPT.slider = slider
        phTest2PPT.css = css
        request.text = Some(phTest2PPT)
        val msg = toJsonapi(request).asJson.toString()
        sendMessage(msg)
    }
}

trait createExcelCss extends phRequest with PharbersInjectModule {
    import com.pharbers.moduleConfig.ModuleConfig.fr
    implicit val f: (ConfigDefines, Node) => ConfigImpl = ((c, n) => ConfigImpl(c.md map (x =>x -> (n \ x))))

    override val md: List[String] = "format" :: "out_file" :: "css_file" :: Nil
    override val id: String = "gen_pages"
    override val configPath: String = "pharbers_config/bi_config.xml"
    override lazy val config: ConfigImpl = loadConfig(configDir + "/" + configPath)

    lazy val cssPath = config.mc.find(_._1 == "css_file").map { iter =>
        (iter._2.asInstanceOf[NodeSeq] \\ "@path").toString()
    }.getOrElse(throw new Exception("配置文件错误，phGenPPT => out_file"))

    def createCss(name:List[String], cell: String): PhExcelCss ={
        val phExcelCss = new PhExcelCss
        phExcelCss.id = UUID.randomUUID().toString
        phExcelCss.cell = cell
        val jsValue = Json.parse(Source.fromFile(cssPath).mkString)
        val cssJs = (jsValue \ name.head).asOpt[JsValue].getOrElse(Json.toJson(""))
        var cssJs2 = Json.toJson("")
        if (name.tail.nonEmpty) cssJs2 = (jsValue \ name.tail.head).asOpt[JsValue].getOrElse(Json.toJson(""))
//        val cssJs2 = (jsValue \ name.tail).asOpt[JsValue].getOrElse(Json.toJson(""))
        phExcelCss.factory = (cssJs \ "factory").asOpt[String].getOrElse((cssJs2 \ "factory").asOpt[String].getOrElse(phExcelCss.factory))
        phExcelCss.fontSize = (cssJs \ "fontSize").asOpt[String].getOrElse((cssJs2 \ "fontSize").asOpt[String].getOrElse( phExcelCss.fontSize))
        phExcelCss.fontColor = (cssJs \ "fontColor").asOpt[String].getOrElse((cssJs2 \ "fontColor").asOpt[String].getOrElse(phExcelCss.fontColor))
        phExcelCss.fontName = (cssJs \ "fontName").asOpt[String].getOrElse((cssJs2 \ "fontName").asOpt[String].getOrElse(phExcelCss.fontName))
        phExcelCss.fontStyle = (cssJs \ "fontStyle").asOpt[List[String]].getOrElse(Nil) ::: (cssJs2 \ "fontStyle").asOpt[List[String]].getOrElse(Nil)
        phExcelCss.cellColor = (cssJs \ "cellColor").asOpt[String].getOrElse((cssJs2 \ "cellColor").asOpt[String].getOrElse(phExcelCss.cellColor))
        phExcelCss.cellBorders = (cssJs \ "cellBorders").asOpt[List[String]].getOrElse(Nil) ::: (cssJs2 \ "cellBorders").asOpt[List[String]].getOrElse(Nil)
        phExcelCss.cellBordersColor = (cssJs \ "cellBordersColor").asOpt[String].getOrElse((cssJs2 \ "cellBordersColor").asOpt[String].getOrElse(phExcelCss.cellBordersColor))
        phExcelCss.width = (cssJs \ "width").asOpt[String].getOrElse((cssJs2 \ "width").asOpt[String].getOrElse(phExcelCss.width))
        phExcelCss.height = (cssJs \ "height").asOpt[String].getOrElse((cssJs2 \ "height").asOpt[String].getOrElse(phExcelCss.height))
        phExcelCss
    }
}