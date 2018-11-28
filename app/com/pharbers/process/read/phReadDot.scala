package com.pharbers.process.read

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.tools.nsc.io.Path

trait phReadDot {
    def getMainTable(dotPath: String, bigTableDFlst: List[DataFrame]): DataFrame = {
        val tmp = phLyFactory.getCalcInstance()
        val dotDF = tmp.readCsv(dotPath).withColumnRenamed("PRODUCT DESC", "PRODUCT DESC_config")
            .withColumnRenamed("PACK DESC", "PACK DESC_config")
            .filter(col("ADD RATE") =!= "N")
        val bigTableDF: DataFrame = bigTableDFlst.reduce((totalDF, df) => totalDF union df)
        val resultDF = bigTableDF.join(dotDF, bigTableDF("PRODUCT DESC") === dotDF("PRODUCT DESC_config") && bigTableDF("PACK DESC") === dotDF("PACK DESC_config"))
            .withColumn("DOT", when(col("UNIT_TYPE") === "ST-CNT.UNIT", col("ST-CNT-UNIT") / col("ADD RATE"))
                .otherwise(when(col("UNIT_TYPE") === "DT-DOS.UNIT", col("DT-DOS-UNIT") / col("ADD RATE"))
                    .otherwise(when(col("UNIT_TYPE") === "UN-T.UNITST", col("UN-T-UNITS") / col("ADD RATE")))))
        println("DOT计算完成=========================")
        resultDF
    }

}

class phReadDotImpl extends phReadDot with phCommand {
    override def exec(args: Any): Any = this.getMainTable(args.asInstanceOf[Map[String, Any]]("dotPath").asInstanceOf[String],
        args.asInstanceOf[Map[String, Any]]("bigTableDF").asInstanceOf[List[DataFrame]])
}