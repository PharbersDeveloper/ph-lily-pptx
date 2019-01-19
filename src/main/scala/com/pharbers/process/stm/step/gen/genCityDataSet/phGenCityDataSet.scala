package com.pharbers.process.stm.step.gen.genCityDataSet

import com.pharbers.baseModules.PharbersInjectModule
import com.pharbers.moduleConfig.{ConfigDefines, ConfigImpl}
import com.pharbers.process.common.{phCommand, phLyCityDataSet, phLyFactory}
import org.apache.spark.rdd.RDD

import scala.xml.{Node, NodeSeq}

trait phGenCityDataSet extends PharbersInjectModule {
    override val id: String = "gen_city_set"
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

    lazy val merge_func = config.mc.find(_._1 == "merges").map { opt =>
        (opt._2.asInstanceOf[NodeSeq] \\ "merge").toList.map { iter =>
            Map(
                "factory" -> (iter \\ "@factory").toString,
                "path" -> (iter \\ "@path").toString,
                "description" -> (iter \\ "@description").toString
            )
        }
    }.getOrElse(throw new Exception("配置文件错误，phGenDataSet => merge func"))
}

class phGenCityDataSetImpl extends phGenCityDataSet with phCommand{
    override def exec(args: Any): Any = {
        val rdd_lst = data_sources.map { nod =>
            val fct = nod.get("factory").get
            val cmd = phLyFactory.getInstance(fct).asInstanceOf[phCommand]
            cmd.exec(nod.get("path").get)
        }

        def callAcc(lst : List[Map[String, String]], rdd : Option[Any]) : Option[Any] = {
            if (lst.isEmpty) rdd
            else {
                val cur = lst.head
                val path = cur.get("path").get
                val cmd = phLyFactory.getInstance(cur.get("factory").get).asInstanceOf[phCommand]
                cmd.preExec(path)
                callAcc(lst.tail, cmd.exec(rdd).asInstanceOf[Option[RDD[(String, phLyCityDataSet)]]])
            }
        }
        val result = callAcc(merge_func, Some(rdd_lst)).get.asInstanceOf[RDD[(String, phLyCityDataSet)]]

        phLyFactory.clearStorage
        phLyFactory.stssoo = phLyFactory.stssoo + ("city-main-frame" -> result)
        val df = phLyFactory.phCityRow2DFDetail("city-main-frame").distinct()
        println("gen data set")
        df.sort(-df("DOT")).show(false)
        println(df.count())
        df
    }
}