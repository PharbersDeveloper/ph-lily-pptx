package com.pharbers.process.read

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

trait phReadDataSixTitle {
    def gettitle(df: DataFrame, unitType: String): List[String] = {
        val pattern1 = "_ ".r
        val pattern2 = "_UNIT".r
        val pattern3 = unitType.r
        val pattern4 = "\\.".r
        val pattern5 = "MTH ~ ".r
        val titleDF = df.na.fill(" ").collectAsList()
        val size: Int = df.limit(1).collect().head.size
        val lst = Array.range(0, size).map(i =>
            pattern5.replaceAllIn(
                pattern4.replaceAllIn(
                    pattern3.replaceAllIn(
                        pattern2.replaceAllIn(pattern1.replaceAllIn(
                            titleDF.get(0).get(i).toString + "_" +
                                titleDF.get(1).get(i).toString + "_" +
                                titleDF.get(2).get(i).toString, ""), ""), "UNIT"), " "), "")
        ).toList
        println(lst)
        lst
    }

    def getBigTable(df:DataFrame): DataFrame = {
        val titlelst = df.columns.toList
        val size = titlelst.size
        val pattern = "_UNIT".r
        val monthlst = titlelst.slice(size - 61, size - 1).map { str => pattern.replaceAllIn(str, "") }
        val resultDF = monthlst.map { i =>
            val title = titlelst.slice(0, 14) ::: List("YM", "Sales", "ST-CNT-UNIT", "DT-DOS-UNIT", "UN-T-UNITS")
            df.withColumn("YM", lit(i))
                .withColumn("ST-CNT-UNIT", when(col("UNIT_TYPE") === "ST-CNT.UNIT", col(i + "_UNIT")).otherwise(0))
                .withColumn("DT-DOS-UNIT", when(col("UNIT_TYPE") === "DT-DOS.UNIT", col(i + "_UNIT")).otherwise(0))
                .withColumn("UN-T-UNITS", when(col("UNIT_TYPE") === "UN-T.UNITS", col(i + "_UNIT")).otherwise(0))
                .withColumnRenamed(i + "_LC-RMB_RENMINBI", "Sales")
                .select(title.head, title.tail: _*)
        }.reduce((totalDF, DF) => totalDF.union(DF))
            .distinct()
        resultDF
    }

    def formatDF(path: String): DataFrame = {
        val tmp = phLyFactory.getCalcInstance()
        import tmp.ss.implicits._
        val df: DataFrame = tmp.ss.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", ",")
            .load(path)
        val titleDF = df.limit(3)
        val title_size = titleDF.collectAsList().get(1).size
        val unitType: String = titleDF.collectAsList().get(1).get(title_size - 1).toString
        val lastCell: String = titleDF.collectAsList().get(0).get(title_size - 1).toString
        val titlelst: List[String] = gettitle(titleDF, unitType)
        val colName: String = titlelst.last
        val resultTitlelst = List("TC II SHORT DESC", "TC II DESC", "TC III SHORT DESC", "TC III DESC", "TC IV SHORT DESC",
            "ATC IV DESC", "COMPS ABB", "COMPS DESC", "PRODUCT SHORT DESC", "PRODUCT DESC", "APP FORM 1 SHORT DESC",
            "APP FORM 1 DESC", "PACK SHORT DESC", "PACK DESC") :::
            titlelst.slice(6, title_size) ::: List("UNIT_TYPE")
        val pharbersColumn: String = "pharbers_column"
        val unionDF: DataFrame =
            titlelst.map(clumnName => List((pharbersColumn)).toDF(clumnName)).reduce((totalDF, df) => totalDF.join(df))
        val hasTitleDF: DataFrame = unionDF.union(df)
            .filter(col(colName) =!= lastCell && col(colName) =!= pharbersColumn).na.drop()
        val resultDF = hasTitleDF.withColumn("TC II SHORT DESC", lit("defult"))
            .withColumn("TC II DESC", lit("defult"))
            .withColumn("TC III SHORT DESC", lit("defult"))
            .withColumn("TC III DESC", lit("defult"))
            .withColumn("TC IV SHORT DESC", lit("defult"))
            .withColumn("ATC IV DESC", lit("defult"))
            .withColumn("APP FORM 1 SHORT DESC", lit("defult"))
            .withColumn("APP FORM 1 DESC", lit("defult"))
            .withColumn("UNIT_TYPE", lit(unitType))
            .withColumnRenamed("COMPS ABBR", "COMPS ABB")
            .select(resultTitlelst.head, resultTitlelst.tail: _*)
        getBigTable(resultDF)
    }
}

class phReadDataSixTitleImpl extends phReadDataSixTitle with phCommand {
    override def exec(args : Any) : Any = this.formatDF(args.asInstanceOf[String])
}