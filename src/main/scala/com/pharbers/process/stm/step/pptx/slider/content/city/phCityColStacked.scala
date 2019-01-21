package com.pharbers.process.stm.step.pptx.slider.content.city

import com.pharbers.process.common.{phCommand, phLyFactory, phLycalData}
import com.pharbers.process.stm.step.pptx.slider.content.phReportTableCol
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class phCityColStacked extends phCommand with phReportTableCol {
    override def exec(args: Any): DataFrame = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val data: DataFrame = argsMap("data").asInstanceOf[DataFrame]
        val displayNamelList: List[String] = argsMap("allDisplayNames").asInstanceOf[List[String]]
        val colList: List[String] = argsMap("colList").asInstanceOf[List[String]]
        val timelineList: List[String] = argsMap("timelineList").asInstanceOf[List[String]]
        val primaryValueName: String = argsMap("primaryValueName").asInstanceOf[String]
        val cityList: List[String] = argsMap("cityList").asInstanceOf[List[String]]
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
            .filter(x => cityList.contains(x.id))
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
                .keyBy(x => (x.id, x.display_name, timeline))
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
            (iter._1._1, iter._2.display_name, iter._1._3, iter._2.result)
        ).toDF("CITY", "DISPLAY_NAME", "TIMELINE", "RESULT")
        result
    }
}

class citySom extends phCommand with phReportTableCol {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val data = argsMap("data").asInstanceOf[DataFrame]
//        val timelineList = argsMap("timelineList").asInstanceOf[List[String]]
        val cityList = argsMap("cityList").asInstanceOf[List[String]]
//        val timeline = timelineList.head
        val resultDF = cityList.map { city =>
            val dataTmp = data.filter(col("CITY") === city)
            val totalResult = dataTmp.select("RESULT").agg("RESULT" -> "sum")
                .collect().head.toString()
                .replaceAll("[\\[\\]]", "")
            dataTmp.withColumn("SOM", (col("RESULT") / totalResult) * 100)
        }.reduce((df1, df2) => df1.union(df2))
        resultDF.na.fill(Double.NaN)
    }
}