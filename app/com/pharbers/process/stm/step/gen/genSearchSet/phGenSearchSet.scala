package com.pharbers.process.stm.step.gen.genSearchSet

import com.pharbers.baseModules.PharbersInjectModule
import com.pharbers.moduleConfig.{ConfigDefines, ConfigImpl}
import com.pharbers.process.common.{phCommand, phLyFactory}

import scala.xml.Node
import scala.xml.NodeSeq

trait phGenSearchSet extends PharbersInjectModule {
    override val id: String = "gen_search_set"
    override val configPath: String = "pharbers_config/bi_config.xml"
    override val md: List[String] = "merge" :: "data_nodes" :: Nil

    import com.pharbers.moduleConfig.ModuleConfig.fr
    implicit val f: (ConfigDefines, Node) => ConfigImpl = ((c, n) => ConfigImpl(c.md map (x =>x -> (n \ x))))
    override lazy val config: ConfigImpl = loadConfig(configDir + "/" + configPath)

    lazy val data_sources = config.mc.find(_._1 == "data_nodes").map { opt =>
        (opt._2.asInstanceOf[NodeSeq] \\ "data_node").toList.map { iter =>
            (iter \\ "@index").toString().toInt -> Map(
                "name" -> (iter \\ "@name").toString(),
                "description" -> (iter \\ "@description").toString(),
                "path" -> (iter \\ "@path").toString(),
                "factory" -> (iter \\ "@factory").toString()
            )
        }
    }.getOrElse(throw new Exception("配置文件错误，phGenSearchSet => data source")).sortBy(_._1).map (_._2)

    lazy val merge_func = config.mc.find(_._1 == "merge").map { opt =>
        Map(
            "factory" -> (opt._2.asInstanceOf[NodeSeq] \\ "@factory").toString(),
            "description" -> (opt._2.asInstanceOf[NodeSeq] \\ "@description").toString()
        )
    }.getOrElse(throw new Exception("配置文件错误，phGenSearchSet => merge func"))
}

class phGenSearchSetImpl extends phGenSearchSet with phCommand {
    override def exec: Unit = {
//        println(data_sources)
        data_sources.foreach(x => {
            val function = x("factory")
            val command = phLyFactory.getInstance(function).asInstanceOf[phCommand]
            command.perExec(x("path"))
            command.exec
            command.postExec
        })
        println(merge_func)
        println("gen search set")
    }
}