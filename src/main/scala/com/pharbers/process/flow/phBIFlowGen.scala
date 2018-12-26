package com.pharbers.process.flow

import com.pharbers.baseModules.PharbersInjectModule
import com.pharbers.moduleConfig.{ConfigDefines, ConfigImpl}
import com.pharbers.process.common.{phCommand, phLyFactory}

import scala.xml.Node
import scala.xml.NodeSeq

trait phBIFlowGen extends PharbersInjectModule {
    override val id: String = "flow_define"
    override val configPath: String = "pharbers_config/bi_config.xml"
    override val md: List[String] = "storage" :: "calculate" :: "flow_nodes" :: Nil

    import com.pharbers.moduleConfig.ModuleConfig.fr
    implicit val f: (ConfigDefines, Node) => ConfigImpl = ((c, n) => ConfigImpl(c.md map (x =>x -> (n \ x))))
    override lazy val config: ConfigImpl = loadConfig(configDir + "/" + configPath)

    lazy val storage = config.mc.find(_._1 == "storage").map { opt =>
        (opt._2.asInstanceOf[NodeSeq] \\ "@factory").toString
    }.getOrElse(throw new Exception("配置文件错误，phBIFlowGen => storage"))

    lazy val cal_driver = config.mc.find(_._1 == "calculate").map { opt =>
        (opt._2.asInstanceOf[NodeSeq] \\ "@factory").toString
    }.getOrElse(throw new Exception("配置文件错误，phBIFlowGen => calculate"))

    lazy val steps = config.mc.find(_._1 == "flow_nodes").map { opt =>
        (opt._2.asInstanceOf[NodeSeq] \\ "flow_node").toList.map { iter =>
            (iter \\ "@index").toString().toInt -> Map(
                "name" -> (iter \\ "@name").toString(),
                "description" -> (iter \\ "@description").toString(),
                "factory" -> (iter \\ "@factory").toString()
            )
        }
    }.getOrElse(throw new Exception("配置文件错误，phBIFlowGen => steps")).sortBy(_._1).map (_._2)
}

class phBIFlowGenImpl extends phBIFlowGen with phCommand {
    override def exec(args : Any) : Any = {
        val jobid = args.asInstanceOf[String]
        /**
          * 1. storage creatation
          */
        println(this.storage)

        /**
          * 2. spark driver creation
          */
        println(this.cal_driver)

        steps.foreach { nod =>
            val ins_name = nod.get("factory").get
            val cmd = phLyFactory.getInstance(ins_name).asInstanceOf[phCommand]
            cmd.exec(jobid)
        }
    }
}
