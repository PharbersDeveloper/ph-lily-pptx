package com.pharbers.process.stm.step.pptx.slider.content.city

import com.pharbers.process.common.{phLyFactory, phLycalArray, phLycalData}
import com.pharbers.spark.phSparkDriver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class phCityColAntiPart extends Serializable {
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
                case "QTR" => 3
            }
            case 2 => timeline.charAt(0) match {
                case 'M' => 1
                case 'R' => 3
                case 'Q' => 3
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
        //Timeline的年月部分
        val timelineListYM = timelineList.map{x =>
            val ymMap = getTimeLineYm(x)
            val month = ymMap("month")
            val year = ymMap("year")
            if (month < 10) year.toString + "0" + month.toString
            else year.toString + month.toString
        }
        val indexList = timelineListYM.map(x => allTimelst.indexOf(x))
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
        val funcFileter: Map[String, phLycalData => Boolean] = Map("rmb" -> filter_func_rmb, "dot" -> filter_func_dot, "LC-RMB" -> filter_func_rmb)
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
        val valueFuncMap: Map[String, phLycalData => BigDecimal] = Map("rmb" -> func_rmb, "LC-RMB" -> func_rmb, "dot" -> func_dot)
        import org.apache.spark.sql.functions.col
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
            }.map(x => (x._1, x._2._2.reVal))
        val func_growth: RDD[(String, List[BigDecimal])] => RDD[(String, String, List[String])] = mid_sum => {
            mid_sum.map { iter =>
                val growth: List[String] = iter._2.zipWithIndex.map { case (value, idx) =>
                    if (indexList.contains(idx)) {
                        val m = iter._2.apply(idx - 12)
                        if (m == 0) "Nan"
                        else (((value - m) / m) * 100).toString()
                    } else {
                        BigDecimal(20181231).toString()
                    }
                }
                val timelineGrowth = indexList.map(x => growth(x))
                (iter._1, "GROWTH", timelineGrowth)
            }
        }
        val func_som: RDD[(String, List[BigDecimal])] => RDD[(String, String, List[String])] = mid_sum => {
            val mktDisplayNameList = mid_sum.filter(x => x._1 == mktDisplayName).collect().head._2
            mid_sum.map { iter =>
                val som = iter._2.zipWithIndex.map { case (value, idx) =>
                    if (indexList.contains(idx)) {
                        val m = mktDisplayNameList(idx)
                        if (m == 0) "NaN"
                        else ((value / mktDisplayNameList(idx)) * 100).toString()
                    } else {
                        BigDecimal(20181231).toString()
                    }
                }
                val timelineSom = indexList.map(x => som(x))
                (iter._1, "SOM", timelineSom)
            }
        }
        val func_value: RDD[(String, List[BigDecimal])] => RDD[(String, String, List[String])] = mid_sum => {
            mid_sum.map { iter =>
                val valueList = indexList.map(x => iter._2(x).toString())
                (iter._1, "RESULT", valueList)
            }
        }
        val funcMap: Map[String, RDD[(String, List[BigDecimal])] => RDD[(String, String, List[String])]] =
            Map("som" -> func_som, "Growth(%)" -> func_growth, "value" -> func_value)
        println("++++++++++++++++++++++++++++")
        mid_sum.take(20).foreach(println)
        val valueResult = funcMap("value")(mid_sum)
        val somResult = funcMap("som")(mid_sum)
        val growthResult = funcMap("Growth(%)")(mid_sum)
        val result = valueResult.union(somResult).union(growthResult)
        result.take(20).foreach(println)
        result
    }
}
