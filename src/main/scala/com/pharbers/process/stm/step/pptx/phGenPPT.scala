package com.pharbers.process.stm.step.pptx

import java.io.FileOutputStream
import java.util.Date

import com.pharbers.baseModules.PharbersInjectModule
import com.pharbers.moduleConfig.{ConfigDefines, ConfigImpl}
import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.poi.xslf.usermodel.XMLSlideShow
import play.api.libs.json._

import scala.io.Source
import scala.xml.{Node, NodeSeq}

trait phGenPPT extends PharbersInjectModule {
    override val id: String = "gen_pages"
    override val configPath: String = "pharbers_config/bi_config.xml"
    override val md: List[String] = "format" :: "out_file" :: Nil

    import com.pharbers.moduleConfig.ModuleConfig.fr
    implicit val f: (ConfigDefines, Node) => ConfigImpl = ((c, n) => ConfigImpl(c.md map (x =>x -> (n \ x))))
    override lazy val config: ConfigImpl = loadConfig(configDir + "/" + configPath)

    lazy val format = config.mc.find(_._1 == "format").map { iter =>
        Map(
            "path" -> (iter._2.asInstanceOf[NodeSeq] \\ "@path").toString,
            "factory" -> (iter._2.asInstanceOf[NodeSeq] \\ "@factory").toString
        )
    }.getOrElse(throw new Exception("配置文件错误，phGenPPT => format"))

    lazy val out_file = config.mc.find(_._1 == "out_file").map { iter =>
        (iter._2.asInstanceOf[NodeSeq] \\ "@path").toString()
    }.getOrElse(throw new Exception("配置文件错误，phGenPPT => out_file"))
}

class phGenPPTImpl extends phGenPPT with phCommand {
    override def exec(args : Any) : Any = {
        println("******************************************************************************************")
        println(new Date())
        val format_filename = this.format.get("path").get
        val buf = Source.fromFile(format_filename)
        val format = Json.parse(buf.mkString)
        val factory = this.format.get("factory").get
        val name = phLyFactory.getInstance(factory).asInstanceOf[phCommand].exec(format)
        val ppt = phLyFactory.stssoo("ppt").asInstanceOf[XMLSlideShow]
        ppt.write(new FileOutputStream(name + ".pptx"))
        println("phGenPPTImpl")
    }
}