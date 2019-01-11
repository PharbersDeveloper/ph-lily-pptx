package com.pharbers.process.read

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame

class phReadSearchImpl extends phCommand {
    override def exec(args: Any): Any = {
        loadDataFromPathInHDFS(args.asInstanceOf[String])
    }

    def loadDataFromPathInHDFS(filepath: String): DataFrame = {
        val tmp = phLyFactory.getCalcInstance()
        tmp.ss.read.format("com.databricks.spark.csv")
            .option("delimiter", ",")
            .option("header", "false")
            .load(filepath.asInstanceOf[String])
            .withColumnRenamed("_c0", "Display Name")
            .withColumnRenamed("_c1", "COMPS_DESC_MARKET")
            .withColumnRenamed("_c2", "PRODUCT_DESC_MARKET")
            .withColumnRenamed("_c3", "PACK_DESC_MARKET")
            .withColumnRenamed("_c4", "name")
            .filter(x => x(1) != "COMPS DESC")
            .filter(x => x(4) != "NAME")
            .select("Display Name", "COMPS_DESC_MARKET", "PRODUCT_DESC_MARKET", "PACK_DESC_MARKET", "name")
    }
}