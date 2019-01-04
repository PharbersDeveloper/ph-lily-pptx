package com.pharbers.process.stm.step.gen.genMktOverViewSet

import com.pharbers.baseModules.PharbersInjectModule
import com.pharbers.moduleConfig.{ConfigDefines, ConfigImpl}
import com.pharbers.process.common.{phCommand, phLyFactory}

import scala.xml.{Node, NodeSeq}

class phGenMOVSet extends PharbersInjectModule {
    override val id: String = "gen_mov_set"
    override val configPath: String = "pharbers_config/bi_config.xml"
    override val md: List[String] = "merges" :: "data_nodes" :: Nil

    import com.pharbers.moduleConfig.ModuleConfig.fr

    implicit val f: (ConfigDefines, Node) => ConfigImpl = ((c, n) => ConfigImpl(c.md map (x => x -> (n \ x))))
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
    }.getOrElse(throw new Exception("配置文件错误，phGenDataSet => data source")).sortBy(_._1).map(_._2)
}

class phGenMOVSetImpl extends phGenMOVSet with phCommand {
    override def exec(args: Any): Any = {
        val rss_lst = data_sources.map { nod =>
            val fct = nod("factory")
            val cmd = phLyFactory.getInstance(fct).asInstanceOf[phCommand]
            cmd.exec(nod("path"))
        }
    }
}