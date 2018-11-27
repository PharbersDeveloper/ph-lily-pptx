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

class phReadSearchImpl extends phReadData with phCommand {
    override def perExec(args: Any): Unit = filepath = args.asInstanceOf[String]
    override def exec: Unit = this.loadDataFromPathInHDFS
    override def loadDataFromPathInHDFS: Unit = {
        val tmp = phLyFactory.getCalcInstance()
        df = Some(tmp.ss.read.format("com.databricks.spark.csv")
                .option("delimiter", ",")
                .load(this.filepath)
                .withColumnRenamed("PRODUCT DESC", "PRODUCT_DESC_MARKET")
                .withColumnRenamed("PACK DESC", "PACK_DESC_MARKET")
                .withColumnRenamed("COMPS DESC", "COMPS_DESC_MARKET")
                .select("Display Name", "COMPS_DESC_MARKET", "PRODUCT_DESC_MARKET", "PACK_DESC_MARKET"))
        phLyFactory.stssoo += ("search" -> df)
    }
}
