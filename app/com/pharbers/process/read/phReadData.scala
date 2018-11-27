package com.pharbers.process.read

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame

trait phReadData {
    var df : Option[DataFrame] = None
    def loadDataFromPathInHDFS(filepath : String) : Unit = {
        val tmp = phLyFactory.getCalcInstance()
        df = Some(tmp.ss.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", ",")
            .load(filepath))
        df.get.show(false)
    }
}

class phReadDataImpl extends phReadData with phCommand {
    override def exec(args : Any) : Any = this.loadDataFromPathInHDFS(args.asInstanceOf[String])
}

