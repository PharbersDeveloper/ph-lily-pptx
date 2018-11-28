package com.pharbers.process.read

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait phReadData {
    val fullTitleList: List[String] = List("TC II SHORT DESC",
        "TC II DESC", "TC III SHORT DESC", "TC III DESC", "TC IV SHORT DESC", "ATC IV DESC", "COMPS_ABB", "COMPS_DESC",
        "PRODUCT SHORT DESC", "PRODUCT DESC", "APP FORM 1 SHORT DESC", "APP FORM 1 DESC", "PACK SHORT DESC", "PACK DESC"
    )
    var df : Option[DataFrame] = None
    def loadDataFromPathInHDFS(filepath : String) : DataFrame = {
        val tmp = phLyFactory.getCalcInstance()
        val sourceDF: DataFrame = tmp.ss.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", ",")
            .load(filepath)
        df = Some(bigTableFormat(formatDF(sourceDF)))
        df.get.show(false)
        df.get
    }

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
        lst
    }

    def replaceTitle(df: DataFrame, titlelst: List[String]): DataFrame = {
        if (titlelst.isEmpty) df
        else replaceTitle(df.withColumnRenamed("_c" + (titlelst.size - 1).toString, titlelst.last), titlelst.init)
    }

    def addColumn(df: DataFrame): DataFrame = {
        import collection.JavaConverters._
        import scala.collection.JavaConversions.asScalaBuffer
        val columnList: java.util.ArrayList[String] = new java.util.ArrayList[String](df.columns.toList.asJava)
        val fullTitleArrayList: java.util.ArrayList[String] = new java.util.ArrayList[String](fullTitleList.asJava)
        fullTitleArrayList.removeAll(columnList)
        val missingColumn: List[String] = fullTitleArrayList.toBuffer.toList
        addColumn1(df, missingColumn)
    }

    def addColumn1(df: DataFrame, titlelst: List[String]): DataFrame = {
        if (titlelst.isEmpty) df
        else addColumn1(df.withColumn(titlelst.head, lit("default")), titlelst.tail)
    }

    def formatDF(df: DataFrame): DataFrame = {
        val titleDF = df.limit(3)
        val title_size = titleDF.collectAsList().get(1).size
        val unitType: String = titleDF.collectAsList().get(1).get(title_size - 1).toString
        val lastCell: String = titleDF.collectAsList().get(0).get(title_size - 1).toString
        val titlelst: List[String] = gettitle(titleDF, unitType)
        val colName: String = titlelst.last
        val resultTitlelst = fullTitleList ::: titlelst.takeRight(120) ::: List("UNIT_TYPE")
        val hasTitleDF: DataFrame = replaceTitle(df, titlelst)
            .withColumnRenamed("COMPS ABBR", "COMPS_ABB")
        val fullTitleDF: DataFrame = addColumn(hasTitleDF)
        val resultDF = fullTitleDF.filter(col(colName) =!= lastCell).na.drop()
            .withColumn("UNIT_TYPE", lit(unitType))
            .select(resultTitlelst.head, resultTitlelst.tail: _*)
        resultDF
    }

    def bigTableFormat(df: DataFrame): DataFrame = {
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
}

class phReadDataImpl extends phReadData with phCommand {
    override def exec(args : Any) : Any = this.loadDataFromPathInHDFS(args.asInstanceOf[String])
}

