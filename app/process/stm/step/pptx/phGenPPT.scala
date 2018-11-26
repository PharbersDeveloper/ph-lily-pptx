package process.stm.step.pptx

import com.pharbers.baseModules.PharbersInjectModule
import process.common.phCommand

trait phGenPPT extends PharbersInjectModule {
    override val id: String = "gen_search_set"
    override val configPath: String = "pharbers_config/bi_config.xml"
    override val md: List[String] = "factory" :: "in_file" :: "out_file" :: Nil

    lazy val factory = config.mc.find(_._1 == "factory").
                            map (_._2.toString).
                            getOrElse(throw new Exception("配置文件错误，phGenPPT => factory"))

    lazy val in_file = config.mc.find(_._1 == "in_file").
                            map (_._2.toString).
                            getOrElse(throw new Exception("配置文件错误，phGenPPT => in_file"))

    lazy val out_file = config.mc.find(_._1 == "out_file").
                            map (_._2.toString).
                            getOrElse(throw new Exception("配置文件错误，phGenPPT => out_file"))
}

class phGenPPTImpl extends phGenPPT with phCommand {
    override def exec: Unit = {
        println("phGenPPTImpl")
    }
}