package com.pharbers.process.stm.step.pptx.slider.content.city

import com.pharbers.process.common.{phLyGLPData, phLycalArray, phLycalData}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class phGlpShare extends phQuarterTableCol{
    override def getValue(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val data: DataFrame = argsMap("data").asInstanceOf[DataFrame]
        val displayNamelList: List[String] = argsMap("allDisplayNames").asInstanceOf[List[String]]
        val displayNamelMap: Map[String, String] = argsMap("replaysDisplayMap").asInstanceOf[Map[String, String]]
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
        //Timeline的年月部分
        val timelineListYM = timelineList.map{x =>
            val ymMap = getTimeLineYm(x)
            val month = ymMap("month")
            val year = ymMap("year")
            if (month < 10) year.toString + "0" + month.toString
            else year.toString + month.toString
        }
        val indexList = timelineListYM.map(x => allTimelst.indexOf(x))
        val rddTemp = data.toJavaRDD.rdd.map(x => phLyGLPData(x(0).toString, x(1).toString, x(2).toString, x(3).toString,
            BigDecimal(x(4).toString), BigDecimal(x(4).toString), BigDecimal(x(5).toString), x(7).toString))
        /**
          * 1. 整理所有需要的 display Name
          */
        val filter_func_dot: phLyGLPData => Boolean = phLyGLPData => {
            phLyGLPData.dot > 0
        }
        val filter_func_rmb: phLyGLPData => Boolean = phLyGLPData => {
            phLyGLPData.tp == "LC-RMB"
        }
        val funcFileter: Map[String, phLyGLPData => Boolean] = Map("rmb" -> filter_func_rmb, "dot" -> filter_func_dot, "LC-RMB" -> filter_func_rmb)
        val filter_display_name = rddTemp.filter(x => displayNamelList.contains(x.TC_II_SHORT) || displayNamelList.contains(x.TC_IV_SHORT))
            .filter(x => x.date >= allTimelst.min)
            .filter(x => x.date <= allTimelst.max)
            .filter(x => funcFileter(primaryValueName)(x))
        val replace_display_name = filter_display_name.map{x =>
            val displayName_key = if (displayNamelList.indexOf(x.TC_IV_SHORT) != -1) x.TC_IV_SHORT else x.TC_II_SHORT
            (displayNamelMap(displayName_key), x)
        }

        val func_rmb: phLyGLPData => BigDecimal = phLyGLPData => {
            phLyGLPData.value
        }
        val func_dot: phLyGLPData => BigDecimal = phLyGLPData => {
            phLyGLPData.dot
        }
        val valueFuncMap: Map[String, phLyGLPData => BigDecimal] = Map("rmb" -> func_rmb, "LC-RMB" -> func_rmb, "dot" -> func_dot)
        val mid_sum = replace_display_name.map { x =>
            val idx = allTimelst.indexOf(x._2.date)
            val lst = if (idx > -1) {
                val ym = x._2.date.takeRight(4)
                val timeline = headstr + ym.takeRight(2) + " " + ym.take(2)
                val key = if (timeline.substring(0, 3) == "YTD") "YTD" else "OTHER"
                val forward = countMap(key)(timeline)
                List.fill(idx)(BigDecimal(0)) :::
                    List.fill(forward)(valueFuncMap(primaryValueName)(x._2)) :::
                    List.fill(allTimelst.length - idx - forward)(BigDecimal(0))
            } else List.fill(allTimelst.length)(BigDecimal(0))
            (x, phLycalArray(lst))
        }.keyBy(_._1._1)
            .reduceByKey { (left, rigth) =>
                val lst = left._2.reVal.zip(rigth._2.reVal).map(x => x._1 + x._2)
                (left._1, phLycalArray(lst))
            }.map(x => (x._1, x._2._2.reVal))
        val func_growth: RDD[(String, List[BigDecimal])] => RDD[(String, List[String])] = mid_sum => {
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
                (iter._1, timelineGrowth)
            }
        }
        val func_som: RDD[(String, List[BigDecimal])] => RDD[(String, List[String])] = mid_sum => {
            val mktDisplayNameList = mid_sum.reduce((rdd1, rdd2) => (mktDisplayName, rdd1._2.zip(rdd2._2).map(x => x._1 + x._2)))._2
//            val mktDisplayNameList = mid_sum.filter(x => x._1 == mktDisplayName).collect().headOption.getOrElse(("",List(BigDecimal(0))))._2
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
                (iter._1, timelineSom)
            }
        }
        val funcMap: Map[String, RDD[(String, List[BigDecimal])] => RDD[(String, List[String])]] =
            Map("som" -> func_som, "Growth(%)" -> func_growth)
        val result = funcMap(valueType)(mid_sum).filter(x => x._1 != mktDisplayName)
        result.take(20).foreach(println)
        result
    }
}
