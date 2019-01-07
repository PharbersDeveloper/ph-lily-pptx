package com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol

import com.pharbers.process.common._
import com.pharbers.process.stm.step.pptx.slider.content.phReportTableCol
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class marketGrowthCol extends phCommand with phReportTableCol {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        data = argsMap("data").asInstanceOf[DataFrame]
        val mapDf = argsMap("mapping").asInstanceOf[DataFrame]
        val displayNamelList: List[String] = argsMap("allDisplayNames").asInstanceOf[List[String]]
        val colList: List[String] = argsMap("colList").asInstanceOf[List[String]]
        val timelineList: List[String] = argsMap("timelineList").asInstanceOf[List[String]]
        val mktDisplayName = argsMap("mktDisplayName").asInstanceOf[String]
        val primaryValueName: String = argsMap("primaryValueName").asInstanceOf[String]
        val valueType = colList.head
        val forward: Int = timelineYmCount(timelineList.head)
        //需要计算出真正的所有timeline
        val allTimelineList: List[String] = if (colList.contains("Growth(%)")) {
            getAllTimeline(timelineList)
        } else {
            timelineList
        }
        //为了筛选数据
        val allTimelst: List[String] = allTimelineList.map { timeline =>
            val startYm: String = getStartYm(timeline)
            val ymMap = getTimeLineYm(timeline)
            val month = ymMap("month").toString.length match {
                case 1 => "0" + ymMap("month")
                case _ => ymMap("month")
            }
            val endYm: String = ymMap("year").toString + month
            List(startYm, endYm)
        }.reduce((lst1, lst2) => (lst1 ::: lst2).distinct).sorted

        /**
          * 1. 整理所有需要的 display Name
          */
        val filter_func_rmb: DataFrame => DataFrame = DataFrame => {
            DataFrame.filter(col("TYPE") === "LC-RMB")
        }
        val normal: phLyMOVData => Boolean = phLyMOVData => {
            true
        }
        val funcFileter = Map("rmb" -> filter_func_rmb)

        data = funcFileter(primaryValueName)(data.filter(col("DATE") >= allTimelst.min && col("DATE") <= allTimelst.max))
                .join(mapDf, data("ID") === mapDf("ID")).select("DISPLAY_NAME", "DATE", "TYPE", "VALUE")

        val rddTemp = data.toJavaRDD.rdd.map(x =>  phLyMOVData(x(0).toString, x(1).toString, x(2).toString, BigDecimal(x(3).toString)))


        val filter_display_name = rddTemp.filter(x => displayNamelList.contains(x.id))


        val mid_sum = filter_display_name.map { x =>
            val idx = allTimelst.indexOf(x.date)
            val lst = if (idx > -1) {
                List.fill(idx)(BigDecimal(0)) :::
                        List.fill(forward)(x.value) :::
                        List.fill(allTimelst.length - idx - forward)(BigDecimal(0))
            } else List.fill(allTimelst.length)(BigDecimal(0))
            (x, phLycalArray(lst))
        }.keyBy(_._1.id)
                .reduceByKey { (left, right) =>
                    val lst = left._2.reVal.zip(right._2.reVal).map(x => x._1 + x._2)
                    (left._1, phLycalArray(lst))
                }.map(x => (x._1, x._2._2.reVal.reverse))

        //        println("***************************************************")
        //        mid_sum.take(20).foreach(println)
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
            val mktDisplayNameList = mid_sum.reduce((left, right) => {
                val lst = left._2.zip(right._2).map(x => x._1 + x._2)
                ("all", lst)
            })._2
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

class marketSomCol extends phCommand with phReportTableCol {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        data = argsMap("data").asInstanceOf[DataFrame]
        val displayNamelList: List[String] = argsMap("allDisplayNames").asInstanceOf[List[String]]
        val colList: List[String] = argsMap("colList").asInstanceOf[List[String]]
        val timelineList: List[String] = argsMap("timelineList").asInstanceOf[List[String]]
        val mktDisplayName = argsMap("mktDisplayName").asInstanceOf[String]
        val primaryValueName: String = argsMap("primaryValueName").asInstanceOf[String]
        val valueType = colList.head
        val forward: Int = timelineYmCount(timelineList.head)
        //需要计算出真正的所有timeline
        val allTimelineList: List[String] = if (colList.contains("Growth(%)")) {
            getAllTimeline(timelineList)
        } else {
            timelineList
        }
        //为了筛选数据
        val allTimelst: List[String] = allTimelineList.map { timeline =>
            val startYm: String = getStartYm(timeline)
            val ymMap = getTimeLineYm(timeline)
            val month = ymMap("month").toString.length match {
                case 1 => "0" + ymMap("month")
                case _ => ymMap("month")
            }
            val endYm: String = ymMap("year").toString + month
            List(startYm, endYm)
        }.reduce((lst1, lst2) => (lst1 ::: lst2).distinct).sorted

        /**
          * 1. 整理所有需要的 display Name
          */
        val filter_func_rmb: DataFrame => DataFrame = DataFrame => {
            DataFrame.filter(col("TYPE") === "LC-RMB")
        }
        val normal: phLyMOVData => Boolean = phLyMOVData => {
            true
        }
        val funcFileter = Map("rmb" -> filter_func_rmb)

        data = funcFileter(primaryValueName)(data.filter(col("DATE") >= allTimelst.min && col("DATE") <= allTimelst.max))

        val rddTemp = data.toJavaRDD.rdd.map(x =>  phLyMOVData(x(0).toString, x(1).toString, x(2).toString, BigDecimal(x(3).toString)))


        val filter_display_name = rddTemp.filter(x => displayNamelList.contains(x.id))


        val mid_sum = filter_display_name.map { x =>
            val idx = allTimelst.indexOf(x.date)
            val lst = if (idx > -1) {
                List.fill(idx)(BigDecimal(0)) :::
                        List.fill(forward)(x.value) :::
                        List.fill(allTimelst.length - idx - forward)(BigDecimal(0))
            } else List.fill(allTimelst.length)(BigDecimal(0))
            (x, phLycalArray(lst))
        }.keyBy(_._1.id)
                .reduceByKey { (left, right) =>
                    val lst = left._2.reVal.zip(right._2.reVal).map(x => x._1 + x._2)
                    (left._1, phLycalArray(lst))
                }.map(x => (x._1, x._2._2.reVal.reverse))

        //        println("***************************************************")
        //        mid_sum.take(20).foreach(println)
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
            val mktDisplayNameList = mid_sum.reduce((left, right) => {
                val lst = left._2.zip(right._2).map(x => x._1 + x._2)
                ("all", lst)
            })._2
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

        println("****************************************************")
        mid_sum.take(20).foreach(println)
        val result = funcMap(valueType)(mid_sum)
        result
    }
}