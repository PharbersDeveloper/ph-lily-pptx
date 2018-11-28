package com.pharbers.process.stm.step.gen.genDataSet

import java.util.UUID

import com.pharbers.baseModules.PharbersInjectModule
import com.pharbers.moduleConfig.{ConfigDefines, ConfigImpl}
import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame

import scala.xml.{Node, NodeSeq}

trait phGenDataSet extends PharbersInjectModule {
    override val id: String = "gen_data_set"
    override val configPath: String = "pharbers_config/bi_config.xml"
    override val md: List[String] = "merge" :: "data_nodes" :: Nil

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

    lazy val merge_func = config.mc.find(_._1 == "merge").map { opt =>
        Map(
            "factory" -> (opt._2.asInstanceOf[NodeSeq] \\ "@factory").toString(),
            "description" -> (opt._2.asInstanceOf[NodeSeq] \\ "@description").toString(),
            "dotPath" -> (opt._2.asInstanceOf[NodeSeq] \\ "@dotPath").toString()
        )
    }.getOrElse(throw new Exception("配置文件错误，phGenDataSet => merge func"))
}

class phGenDataSetImpl extends phGenDataSet with phCommand {
    override def exec(args: Any): Any = {
        val bigTableDFlst = data_sources.map { nod =>
            val fct = nod.get("factory").get
            val cmd = phLyFactory.getInstance(fct).asInstanceOf[phCommand]
            cmd.exec(nod.get("path").get)
        }

        val cmd = phLyFactory.getInstance(merge_func("factory")).asInstanceOf[phCommand]
        val result = cmd.exec(Map("dotPath" -> merge_func("dotPath"), "bigTableDF" -> bigTableDFlst))
        //        println("写入开始=======================")
        //        result.asInstanceOf[DataFrame].write
        //            .format("csv")
        //            .option("header", value = true)
        //            .option("delimiter", ",")
        //            .save("hdfs:///test/bigTableResult2018112711815")
        //        println("写入完成=======================")
        println("计算count开始=======================")
        val count = result.asInstanceOf[DataFrame].count()
        println("计算count结束=======================")
        println(count)

        //        result.asInstanceOf[DataFrame].show(false)
        //        println(data_sources)
        //        println(merge_func)
        println("gen data set")
        phLyFactory.stssoo
    }
}
