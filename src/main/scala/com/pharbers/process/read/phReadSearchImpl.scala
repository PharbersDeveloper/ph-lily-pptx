package com.pharbers.process.read

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame

class phReadSearchImpl extends phReadData with phCommand {
    override def exec(args : Any): Any = {
        this.loadDataFromPathInHDFS(args.asInstanceOf[String])
        this.df
    }
    override def loadDataFromPathInHDFS(filepath : String): DataFrame = {
        val tmp = phLyFactory.getCalcInstance()
        df = Some(tmp.ss.read.format("com.databricks.spark.csv")
                .option("delimiter", ",")
                .option("header", "true")
                .load(filepath.asInstanceOf[String])
                .withColumnRenamed("PRODUCT DESC", "PRODUCT_DESC_MARKET")
                .withColumnRenamed("PACK DESC", "PACK_DESC_MARKET")
                .withColumnRenamed("COMPS DESC", "COMPS_DESC_MARKET")
                .select("Display Name", "COMPS_DESC_MARKET", "PRODUCT_DESC_MARKET", "PACK_DESC_MARKET"))
        df.get
    }
}