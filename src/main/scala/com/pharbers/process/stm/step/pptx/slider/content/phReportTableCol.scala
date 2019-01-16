package com.pharbers.process.stm.step.pptx.slider.content

import com.pharbers.process.common._
import com.pharbers.spark.phSparkDriver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.collection.mutable

trait phReportTableCol extends Serializable{
    lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
    var data: DataFrame = _

    //获取timeline开始月份
    def getStartYm(timeline: String): String = {
        val ymMap: Map[String, Int] = getTimeLineYm(timeline)
        val month = ymMap("month")
        val year = ymMap("year")
        val ymcount = timelineYmCount(timeline)
        getymlst(month, year, ymcount - 1)
    }

    def getymlst(month: Int, year: Int, ymcount: Int): String = {
        if (ymcount == 0) {
            if (month < 10) {
                year.toString + "0" + month.toString
            } else {
                year.toString + month.toString
            }
        } else {
            if (month == 1) getymlst(12, year - 1, ymcount - 1)
            else getymlst(month - 1, year, ymcount - 1)
        }
    }

    //计算timeline需要前推多少个月份
    def timelineYmCount(timeline: String): Int = {
        val month = getTimeLineYm(timeline)("month")
        timeline.split(" ").length match {
            case 3 => timeline.split(" ")(0) match {
                case "MAT" => 12
                case "YTD" => month
                case "RQ" => 3
            }
            case 2 => timeline.charAt(0) match {
                case 'M' => 1
                case 'R' => 3
            }
        }
    }

    def getTimeLineYm(timeline: String): Map[String, Int] = {
        val ym = timeline.takeRight(5).split(" ")
        val month = ym.head.toInt
        val year = 2000 + ym.last.toInt
        Map("month" -> month, "year" -> year)
    }

    //计算这张表总共前推多少个月份
    def dfMonthCount(timelinelst: List[String], collst: List[String]): Int = {
        val timelineCount = timelinelst.size
        val colMap: Map[String, Int] = Map("RMB" -> 1, "SOM(%)" -> 1, "Grouth(%)" -> 2)
        val timelineMax: Int = timelinelst.map(timeline => timelineYmCount(timeline)).max
        val colMax: Int = collst.map(col => colMap(col)).max
        val monthCount = timelineMax * colMax * timelineCount
        monthCount
    }

    def getAllTimeline(timelineList: List[String]): List[String] = {
        val resultList = timelineList ::: timelineList.map { timeline =>
            val lastYear = (timeline.split(" ").last.toInt - 1).toString
            val lastTimeLine = (timeline.split(" ").take(timeline.split(" ").length - 1) ++ Array(lastYear)).mkString(" ")
            lastTimeLine
        }
        resultList.distinct
    }

    def getAllym(year: Int, month: Int, forward: Int, lst: List[String]): List[String] = {
        if (forward == 0) {
            lst
        }
        else {
            val ym = if (month < 10) {
                year.toString + "0" + month.toString
            } else {
                year.toString + month.toString
            }
            if (month == 1) getAllym(year - 1, 12, forward - 1, lst ::: List(ym))
            else getAllym(year, month - 1, forward - 1, lst ::: List(ym))
        }
    }

    def getValue(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val data: DataFrame = argsMap("data").asInstanceOf[DataFrame]
        val displayNamelList: List[String] = argsMap("allDisplayNames").asInstanceOf[List[String]]
        val colList: List[String] = argsMap("colList").asInstanceOf[List[String]]
        val timelineList: List[String] = argsMap("timelineList").asInstanceOf[List[String]]
        val mktDisplayName = argsMap("mktDisplayName").asInstanceOf[String]
        val primaryValueName: String = argsMap("primaryValueName").asInstanceOf[String]
        val headstr = timelineList.head.dropRight(5)
        val valueType = colList.head
        //需要计算出真正的所有timeline
        val allTimelineList: List[String] = if (colList.contains("Growth(%)")) {
            getAllTimeline(timelineList)
        } else {
            timelineList
        }
        val funcYTD: String => Int = timeline => {
            13 - timelineYmCount(timeline)
        }
        val funcOther: String => Int = timeline => {
            timelineYmCount(timeline)
        }
        val countMap: Map[String, String => Int] = Map("YTD" -> funcYTD, "OTHER" -> funcOther)
        //为了筛选数据
        val allTimelst: List[String] = allTimelineList.map { timeline =>
            val ymMap = getTimeLineYm(timeline)
            val month = ymMap("month").toString.length match {
                case 1 => "0" + ymMap("month")
                case _ => ymMap("month")
            }
            val endYm: String = ymMap("year").toString + month
            val forward = timelineYmCount(timeline)
            getAllym(ymMap("year"), ymMap("month"), forward, List(endYm))
        }.reduce((lst1, lst2) => (lst1 ::: lst2).distinct).sorted
        val rddTemp = data.toJavaRDD.rdd.map(x => phLycalData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
            BigDecimal(x(5).toString), BigDecimal(x(6).toString), BigDecimal(x(7).toString), x(8).toString))
        /**
          * 1. 整理所有需要的 display Name
          */
        val filter_func_dot: phLycalData => Boolean = phLycalData => {
            phLycalData.dot > 0
        }
        val filter_func_rmb: phLycalData => Boolean = phLycalData => {
            phLycalData.tp == "LC-RMB"
        }
        val funcFileter: Map[String, phLycalData => Boolean] = Map("rmb" -> filter_func_rmb, "dot" -> filter_func_dot)
        val filter_display_name = rddTemp.filter(x => displayNamelList.contains(x.display_name))
            .filter(x => x.date >= allTimelst.min)
            .filter(x => x.date <= allTimelst.max)
            .filter(x => funcFileter(primaryValueName)(x))
        val func_rmb: phLycalData => BigDecimal = phLycalData => {
            phLycalData.value
        }
        val func_dot: phLycalData => BigDecimal = phLycalData => {
            phLycalData.dot
        }
        val valueFuncMap: Map[String, phLycalData => BigDecimal] = Map("rmb" -> func_rmb, "dot" -> func_dot)
        val mid_sum = filter_display_name.map { x =>
            val idx = allTimelst.indexOf(x.date)
            val lst = if (idx > -1) {
                val ym = x.date.takeRight(4)
                val timeline = headstr + ym.takeRight(2) + " " + ym.take(2)
                val key = if (timeline.substring(0, 3) == "YTD") "YTD" else "OTHER"
                val forward = countMap(key)(timeline)
                List.fill(idx)(BigDecimal(0)) :::
                    List.fill(forward)(valueFuncMap(primaryValueName)(x)) :::
                    List.fill(allTimelst.length - idx - forward)(BigDecimal(0))
            } else List.fill(allTimelst.length)(BigDecimal(0))
            (x, phLycalArray(lst))
        }.keyBy(_._1.display_name)
            .reduceByKey { (left, rigth) =>
                val lst = left._2.reVal.zip(rigth._2.reVal).map(x => x._1 + x._2)
                (left._1, phLycalArray(lst))
            }.map(x => (x._1, x._2._2.reVal.reverse))
        val func_growth: RDD[(String, List[BigDecimal])] => RDD[(String, List[String])] = mid_sum => {
            mid_sum.map { iter =>
                val growth: List[String] = iter._2.zipWithIndex.map { case (value, idx) =>
                    if (idx >= timelineList.length) {
                        BigDecimal(20181231).toString()
                    } else {
                        val m = iter._2.apply(idx + 12)
                        if (m == 0) "Nan"
                        else (((value - m) / m) * 100).toString()
                    }
                }
                (iter._1, growth.take(timelineList.length).reverse)
            }
        }
        val func_som: RDD[(String, List[BigDecimal])] => RDD[(String, List[String])] = mid_sum => {
            val mktDisplayNameList = mid_sum.filter(x => x._1 == mktDisplayName).collect().head._2
            mid_sum.map { iter =>
                val som = iter._2.zipWithIndex.map { case (value, idx) =>
                    if (idx >= timelineList.length) {
                        BigDecimal(20181231).toString()
                    } else {
                        val m = mktDisplayNameList(idx)
                        if (m == 0) "NaN"
                        else ((value / mktDisplayNameList(idx)) * 100).toString()
                    }
                }
                (iter._1, som.take(timelineList.length).reverse)
            }
        }
        val funcMap: Map[String, RDD[(String, List[BigDecimal])] => RDD[(String, List[String])]] =
            Map("som" -> func_som, "Growth(%)" -> func_growth)
        val result = funcMap(valueType)(mid_sum)
        result
    }
}

class valueDF extends phCommand with phReportTableCol {
    override def exec(args: Any): DataFrame = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val data: DataFrame = argsMap("data").asInstanceOf[DataFrame]
        val displayNamelList: List[String] = argsMap("allDisplayNames").asInstanceOf[List[String]]
        val colList: List[String] = argsMap("colList").asInstanceOf[List[String]]
        val timelineList: List[String] = argsMap("timelineList").asInstanceOf[List[String]]
        val primaryValueName: String = argsMap("primaryValueName").asInstanceOf[String]
        val allTimelineList: List[String] = if (colList.contains("Growth(%)")) {
            getAllTimeline(timelineList)
        } else {
            timelineList
        }
        val allTimelst: List[String] = allTimelineList.map { timeline =>
            val startYm: String = getStartYm(timeline)
            val ymMap = getTimeLineYm(timeline)
            val month = ymMap("month").toString.length match {
                case 1 => "0" + ymMap("month")
                case _ => ymMap("month")
            }
            val endYm: String = ymMap("year").toString + month
            List(startYm, endYm)
        }.reduce((lst1, lst2) => lst1 ::: lst2)
        val rddTemp = data.toJavaRDD.rdd.map(x => phLycalData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
            BigDecimal(x(5).toString), BigDecimal(x(6).toString), BigDecimal(x(7).toString), x(8).toString))
        /**
          * 1. 整理所有需要的 display Name
          */
        val func_rmb: phLycalData => Boolean = phLycalData => {
            phLycalData.tp == primaryValueName
        }
        val func_dot: phLycalData => Boolean = phLycalData => {
            phLycalData.dot >= 0
        }
        val filterMap = Map("LC-RMB" -> func_rmb, "dot" -> func_dot)
        val filter_display_name = rddTemp.filter(x => displayNamelList.contains(x.display_name))
            .filter(x => x.date >= allTimelst.min)
            .filter(x => x.date <= allTimelst.max)
            .filter(x => filterMap(primaryValueName)(x))


        /**
          * 2. reduce by key 就是以display name 求和, 叫中间和
          */

        val func_rmb_value: phLycalData => BigDecimal = phLycalData => {
            phLycalData.value
        }
        val func_dot_value: phLycalData => BigDecimal = phLycalData => {
            phLycalData.dot
        }
        val valueMap = Map("LC-RMB" -> func_rmb_value, "dot" -> func_dot_value)
        val mid_sum = allTimelineList.map { timeline =>
            val startYm: String = getStartYm(timeline)
            val ymMap = getTimeLineYm(timeline)
            val month = ymMap("month").toString.length match {
                case 1 => "0" + ymMap("month")
                case _ => ymMap("month")
            }
            val endYm: String = ymMap("year").toString + month
            filter_display_name.filter(x => x.date >= startYm)
                .filter(x => x.date <= endYm)
                .keyBy(x => (x.display_name, timeline))
                .map { x =>
                    x._2.result = valueMap(primaryValueName)(x._2)
                    x
                }
                .reduceByKey { (left, right) =>
                    left.result = left.result + right.result
                    left
                }
        }.reduce((rdd1, rdd2) => rdd1.union(rdd2))

        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        val result: DataFrame = mid_sum.map(iter =>
            (iter._2.display_name, iter._1._2, iter._2.result)
        ).toDF("DISPLAY_NAME", "TIMELINE", "RESULT")
        result
    }
}

class growthTable extends phCommand with phReportTableCol {
    override def exec(args: Any): Any = getValue(args)
}

class som extends phCommand with phReportTableCol {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val mktDisplayName = argsMap("mktDisplayName").asInstanceOf[String]
        val data = argsMap("data").asInstanceOf[DataFrame]
        val timelineList = argsMap("timelineList").asInstanceOf[List[String]]
        val resultDF = timelineList.map { timeline =>
            val dataTmp = data.filter(col("TIMELINE") === timeline)
            val totalResult = dataTmp.filter(col("DISPLAY_NAME") === mktDisplayName)
                .select("RESULT")
                .collect().head.toString()
                .replaceAll("[\\[\\]]", "")
            dataTmp.withColumn("SOM in " + mktDisplayName, (col("RESULT") / totalResult) * 100)
        }.reduce((df1, df2) => df1.union(df2))
        resultDF.na.fill(Double.NaN)
    }
}

class growthContribution extends phCommand with phReportTableCol {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val mktDisplayName = argsMap("mktDisplayName").asInstanceOf[String]
        val data = argsMap("data").asInstanceOf[DataFrame]
        val timelineList = argsMap("timelineList").asInstanceOf[List[String]]
        val resultDF = timelineList.map { timeline =>
            val dataTmp = data.filter(col("TIMELINE") === timeline)
            val totalResult = dataTmp.filter(col("DISPLAY_NAME") === mktDisplayName)
                .select("RESULT", "GROWTH")
                .collect().head.toSeq
                .reduce((x, y) => (x.toString.toDouble * y.toString.toDouble) / (y.toString.toDouble / 100 + 1))
            dataTmp.withColumn("GROWTH_CONTRIBUTION", (((col("RESULT") * col("GROWTH")) / (col("GROWTH") / 100 + 1)) / totalResult) * 100)
        }.reduce((df1, df2) => df1.union(df2))
        resultDF.na.fill(Double.NaN)
    }
}

class growth extends phCommand with phReportTableCol {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val data = argsMap("data").asInstanceOf[DataFrame]
        val timelineList = argsMap("timelineList").asInstanceOf[List[String]]
        val tmpRDD = data.toJavaRDD.rdd.map(x => phLyGrowthData(x(0).toString, x(1).toString, x(2).toString.toDouble,
            0.toString.toDouble))
        val resultRDD = timelineList.map { timeline =>
            val allTimelineList = getAllTimeline(List(timeline))
            tmpRDD.filter(x => allTimelineList.contains(x.timeline))
                .keyBy(x => x.display_name)
                .reduceByKey { (left, right) =>
                    if (left.timeline == timeline) {
                        left.growth = (left.result - right.result) / right.result * 100
                        left
                    } else {
                        right.growth = (right.result - left.result) / left.result * 100
                        right
                    }
                }
        }.reduce((rdd1, rdd2) => rdd1.union(rdd2))
        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        val resultDF = resultRDD.map(iter =>
            (iter._2.display_name, iter._2.timeline, iter._2.result, iter._2.growth)
        ).toDF("DISPLAY_NAME", "TIMELINE", "RESULT", "GROWTH")
        resultDF
    }
}

class rank extends phCommand with phReportTableCol {
    override def exec(args: Any): DataFrame = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val data = argsMap("data").asInstanceOf[DataFrame]
        val timelineList = argsMap("timelineList").asInstanceOf[List[String]]
        val primaryValueName: String = argsMap("primaryValueName").asInstanceOf[String]
        val allTimelst: List[String] = timelineList.map { timeline =>
            val startYm: String = getStartYm(timeline)
            val ymMap = getTimeLineYm(timeline)
            val month = ymMap("month").toString.length match {
                case 1 => "0" + ymMap("month")
                case _ => ymMap("month")
            }
            val endYm: String = ymMap("year").toString + month
            List(startYm, endYm)
        }.reduce((lst1, lst2) => lst1 ::: lst2)
        val rddTemp = data.toJavaRDD.rdd.map(x => phLyMOVData(x(0).toString, x(1).toString, x(2).toString,
            BigDecimal(x(3).toString)))
        val filter_display_name = rddTemp.filter(x => x.date >= allTimelst.min)
            .filter(x => x.date <= allTimelst.max)
            .filter(x => x.tp == primaryValueName)
        val mid_sum = timelineList.map { timeline =>
            val startYm: String = getStartYm(timeline)
            val ymMap = getTimeLineYm(timeline)
            val month = ymMap("month").toString.length match {
                case 1 => "0" + ymMap("month")
                case _ => ymMap("month")
            }
            val endYm: String = ymMap("year").toString + month
            filter_display_name.filter(x => x.date >= startYm)
                .filter(x => x.date <= endYm)
                .map(x => {
                    x.result = x.value
                    x
                })
                .keyBy(x => (x.id, timeline))
                .reduceByKey { (left, right) =>
                    left.result =
                        (if (left.result == 0) {
                            left.value
                        } else left.result) +
                            (if (right.result == 0) {
                                right.value
                            } else right.result)
                    left
                }
        }.reduce((rdd1, rdd2) => rdd1.union(rdd2))
        val mktDisplayNameSum = mid_sum.map(x => x._2.result).sum()
        val resultList = mid_sum.map(x => (x._1._2, x._2.id, (x._2.result / mktDisplayNameSum) * 100))
            .filter(x => x._2 != "* Others *")
            .sortBy(x => -x._3)
        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        val resultDF = resultList.toDF("TIMELINE", "DISPLAY_NAME", "SOM")
        resultDF
    }
}