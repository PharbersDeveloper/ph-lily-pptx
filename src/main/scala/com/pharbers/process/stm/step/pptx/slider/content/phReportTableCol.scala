package com.pharbers.process.stm.step.pptx.slider.content

import com.pharbers.process.common.{phCommand, phLyFactory}
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


trait phReportTableCol {
    lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()

    import sparkDriver.ss.implicits._

    var data: DataFrame = _

    def getDot(args: Any): Double ={
        val argMap = args.asInstanceOf[Map[String, Any]]
        data = argMap("data").asInstanceOf[DataFrame]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ym = argMap("ym").asInstanceOf[String]
        val startYm = argMap("startYm").asInstanceOf[String]
        val lastYm = argMap("lastYm").asInstanceOf[String]
        val dataMap: mutable.Map[String, Double] = argMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val sum = data.filter(col("Display Name") === displayName)
            .filter(col("DATE") >= startYm)
            .filter(col("DATE") <= lastYm)
            .select("DOT")
            .filter(col("DOT") >= 0)
            .agg(Map("DOT" -> "sum"))
            .collectAsList().get(0)
        var resultSum = 0.0
        if (!sum.anyNull) resultSum = sum.toString().substring(1, sum.toString().length - 1).toDouble
        dataMap(displayName + ym) = resultSum
        resultSum
    }

    def getRMB(args: Any): Double = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        data = argMap("data").asInstanceOf[DataFrame]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ym = argMap("ym").asInstanceOf[String]
        val startYm = argMap("startYm").asInstanceOf[String]
        val lastYm = argMap("lastYm").asInstanceOf[String]
        val dataMap: mutable.Map[String, Double] = argMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val sum = data.filter(col("Display Name") === displayName)
            .filter(col("TYPE") === "LC-RMB")
            .filter(col("DATE") >= startYm)
            .filter(col("DATE") <= lastYm)
            .select("VALUE")
            .filter(col("VALUE") >= 0)
            .agg(Map("VALUE" -> "sum"))
            .collectAsList().get(0)
        var resultSum: Double = 0.0
        if (!sum.anyNull) resultSum = sum.toString().substring(1, sum.toString().length - 1).toDouble
        dataMap(displayName + ym) = resultSum
        resultSum
    }

    def getTable(args: Any): Double = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        data = argMap("data").asInstanceOf[DataFrame]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ym = argMap("ym").asInstanceOf[String]
        val startYm = argMap("startYm").asInstanceOf[String]
        val lastYm = argMap("lastYm").asInstanceOf[String]
        val dataMap: mutable.Map[String, Double] = argMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val sum = data.filter(col("Display Name") === displayName)
                .filter(col("TYPE") === "ST-CNT.UNIT")
                .filter(col("DATE") >= startYm)
                .filter(col("DATE") <= lastYm)
                .select("VALUE")
                .filter(col("VALUE") >= 0)
                .agg(Map("VALUE" -> "sum"))
                .collectAsList().get(0)
        var resultSum = 0.0
        if (!sum.anyNull) resultSum = sum.toString().substring(1, sum.toString().length - 1).toDouble
        dataMap(displayName + ym) = resultSum
        resultSum.toLong
    }
}

class dot extends phReportTableCol with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        val dataMap: mutable.Map[String, Double] = argMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ym = argMap("ym").asInstanceOf[String]
        val resultSum = dataMap.getOrElse(displayName + ym, getDot(args))
        resultSum.toString
    }
}

class dotMn extends phReportTableCol with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        val dataMap: mutable.Map[String, Double] = argMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ym = argMap("ym").asInstanceOf[String]
        val resultSum = dataMap.getOrElse(displayName + ym, getDot(args))
        dataMap(displayName + ym) = resultSum / 1000000
        (resultSum / 1000000).toString
    }
}

class GrowthPercentage extends phReportTableCol with phCommand {
    override def exec(args: Any): Double = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        data = argMap("data").asInstanceOf[DataFrame]
        val dataMap = argMap("dataMap").asInstanceOf[collection.mutable.Map[String, Double]]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ym = argMap("ym").asInstanceOf[String]
        val startYm = argMap("startYm").asInstanceOf[String]
        val lastYm = argMap("lastYm").asInstanceOf[String]
        val firstColName = argMap("firstCol").asInstanceOf[String]
        val yearSum: Double = argMap("dataMap").asInstanceOf[mutable.Map[String, Double]](displayName + ym)
        val lastYear = (ym.split(" ").last.toInt - 1).toString
        val lastTimeLine = (ym.split(" ").take(ym.split(" ").length - 1) ++ Array(lastYear)).mkString(" ")
        //        val lastymstr: String = month + " " + lastYear
        //        val lastyearymDF: DataFrame = getYmDF("MAT M"+month+" "+lastYear)
        val lastYear_startYm = (startYm.substring(0, 4).toInt -1).toString + startYm.takeRight(2)
        val lastYear_lastYm = (lastYm.substring(0, 4).toInt -1).toString + lastYm.takeRight(2)
        val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(firstColName)
        val lastYearResult = argMap("dataMap").asInstanceOf[mutable.Map[String, Double]].getOrElse(displayName + lastTimeLine, {
            phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
                Map("data" -> data,
                    "displayName" -> displayName,
                    "ym" -> lastTimeLine,
                    "dataMap" -> dataMap,
                    "firstRow" -> "",
                    "firstCol" -> "",
                    "startYm" -> lastYear_startYm,
                    "lastYm" -> lastYear_lastYm
                )
            ).asInstanceOf[String].toDouble
        })
        argMap("dataMap").asInstanceOf[mutable.Map[String, Double]](displayName + lastTimeLine) = lastYearResult
        ((yearSum - lastYearResult) / lastYearResult) * 100
    }
}

class som extends phReportTableCol with phCommand {
    override def exec(args: Any): Double = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        data = argMap("data").asInstanceOf[DataFrame]
        val map = argMap("dataMap").asInstanceOf[collection.mutable.Map[String, Double]]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ym = argMap("ym").asInstanceOf[String]
        val firstDisplayName = argMap("firstRow").asInstanceOf[String]
        map(displayName + ym) / map(firstDisplayName + ym) * 100
    }
}

class rmb extends phReportTableCol with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        val dataMap: mutable.Map[String, Double] = argMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ym = argMap("ym").asInstanceOf[String]
        val resultSum = dataMap.getOrElse(displayName + ym, getRMB(args))
        resultSum.toLong.toString
    }
}

class rmbMn extends phReportTableCol with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        val dataMap: mutable.Map[String, Double] = argMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ym = argMap("ym").asInstanceOf[String]
        val resultSum = dataMap.getOrElse(displayName + ym, getRMB(args))

        dataMap(displayName + ym) = resultSum / 1000000
        (resultSum / 1000000).toString
    }
}

class tablet extends phReportTableCol with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        val dataMap: mutable.Map[String, Double] = argMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ym = argMap("ym").asInstanceOf[String]
        val resultSum = dataMap.getOrElse(displayName + ym, getTable(args))
        resultSum.toString
    }
}

class empty extends phReportTableCol with phCommand {
    override def exec(args: Any): String = {
        ""
    }
}

