package com.pharbers.process.read

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame

trait phReadData {
    var filepath : String = ""
    var df : Option[DataFrame] = None
    def loadDataFromPathInHDFS : Unit = {
        val tmp = phLyFactory.getCalcInstance()
        df = Some(tmp.ss.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", ",")
            .load(this.filepath))
//        df.get.show(false)
    }
}

class phReadDataImpl extends phReadData with phCommand {
    override def perExec(args: Any): Unit = filepath = args.asInstanceOf[String]
    override def exec: Unit = this.loadDataFromPathInHDFS
}

