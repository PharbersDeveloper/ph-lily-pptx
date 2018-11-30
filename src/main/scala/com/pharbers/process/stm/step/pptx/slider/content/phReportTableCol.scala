package com.pharbers.process.stm.step.pptx.slider.content

import com.pharbers.process.common.{phCommand, phLyFactory}
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


trait phReportTableCol{
    lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
    import sparkDriver.ss.implicits._

    var data: DataFrame = _
    def getYmDF(ymstr: String): DataFrame = {
//        val mat = "MAT M[0-9][0-9] [0-9][0-9]".r
//        val mon = "[M,MON,MTH] M[0-9][0-9] [0-9][0-9]".r
        ymstr.split(" ").length match {
            case 3 => {
                ymstr.split(" ")(0) match {
                    case "MAT" => {
                        val ym = ymstr.substring(5).split(" ")
                        val month = ym(0).toInt
                        val year = 2000 + ym(1).toInt
                        getymlst(List(), month, year, 12).map { str =>
                            if (str.length == 7) str
                            else "0" + str
                        }.toDF("yms")
                    }
                    case "YTD" => {
                        val ym = ymstr.substring(5).split(" ")
                        val month = ym(0).toInt
                        val year = 2000 + ym(1).toInt
                        (1 to month).map(x => s"$x/$year").map { str =>
                            if (str.length == 7) str
                            else "0" + str
                        }.toDF("yms")
                    }
                    case "RQ" => {
                        val ym = ymstr.substring(5).split(" ")
                        val month = ym(0).toInt
                        val year = 2000 + ym(1).toInt
                        getymlst(List(), month, year, 3).map { str =>
                            if (str.length == 7) str
                            else "0" + str
                        }.toDF("yms")
                    }
                }
            }
            case 2 => {
                ymstr.charAt(0) match {
                    case 'M' => {
                        val ym = ymstr.substring(1).split(" ")
                        val month = ym(0).toInt
                        val year = 2000 + ym(1).toInt
                        List(s"$month/$year").map { str =>
                            if (str.length == 7) str
                            else "0" + str
                        }.toDF("yms")
                    }
                }
            }
        }
    }
    def getymlst(ymlst: List[String], month: Int, year: Int, ymcount: Int): List[String] = {
        if (ymlst.size == ymcount) {
            ymlst
        } else {
            if (month == 1) getymlst(ymlst ::: List(month + "/" + year), 12, year - 1, ymcount)
            else getymlst(ymlst ::: List(month + "/" + year), month - 1, year, ymcount)
        }
    }
}

class dotMn extends  phReportTableCol with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        data = argMap("data").asInstanceOf[DataFrame]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ym = argMap("ym").asInstanceOf[String]
        val dataMap: mutable.Map[String, Double] = argMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val ymDF = getYmDF(ym)
        val sum = data.filter(col("Display Name") === displayName)
                .join(ymDF, data("DATE") === ymDF("yms"))
                .select("DOT")
                .filter(col("DOT") > 0)
                .agg(Map("DOT" -> "sum"))
                .collectAsList().get(0).toString()
        val resultSum = sum.substring(1,sum.length-1).toDouble
        dataMap(displayName+ym) = resultSum
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
        val firstColName = argMap("firstCol").asInstanceOf[String]
        val yearSum: Double = argMap("dataMap").asInstanceOf[mutable.Map[String, Double]](displayName+ym)
        val lastYear = (ym.split(" ").last.toInt-1).toString
        val lastYm = (ym.split(" ").take(ym.split(" ").length - 1) ++ Array(lastYear)).mkString(" ")
//        val lastymstr: String = month + " " + lastYear
//        val lastyearymDF: DataFrame = getYmDF("MAT M"+month+" "+lastYear)
        val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(firstColName)
        val lastYearResult = phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
            Map("data" -> data,
                "displayName" -> displayName,
                "ym" -> lastYm,
                "dataMap" -> dataMap,
                "firstRow" -> "",
                "firstCol" -> ""
            )
        ).asInstanceOf[String].toDouble
//        val lastYearSum = data.filter(col("Display Name") === displayName)
//            .join(lastyearymDF, data("DATE") === lastyearymDF("yms"))
//            .select("DOT")
//            .filter(col("DOT") > 0)
//            .agg(Map("DOT" -> "sum"))
//            .collectAsList().get(0).toString()
//        val lasteYearResult = lastYearSum.substring(1,lastYearSum.size-1).toDouble
        ((yearSum-lastYearResult)/lastYearResult)*100
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
        data = argMap("data").asInstanceOf[DataFrame]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ym = argMap("ym").asInstanceOf[String]
        val dataMap: mutable.Map[String, Double] = argMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val ymDF = getYmDF(ym)
        val sum = data.filter(col("Display Name") === displayName)
                .filter(col("TYPE") === "LC-RMB")
                .join(ymDF, data("DATE") === ymDF("yms"))
                .select("VALUE")
                .filter(col("VALUE") > 0)
                .agg(Map("VALUE" -> "sum"))
                .collectAsList().get(0).toString()
        val resultSum = sum.substring(1,sum.length-1).toDouble
        dataMap(displayName+ym) = resultSum
        resultSum.toLong.toString
    }
}
