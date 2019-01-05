package com.pharbers.process.stm.step.pptx.slider.content.overview.col

import com.pharbers.process.common._
import com.pharbers.process.stm.step.pptx.slider.content.phReportTableCol
import com.pharbers.spark.phSparkDriver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class lilyGroupGrowthCol extends phCommand with phReportTableCol {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val dataMap = argsMap("data").asInstanceOf[Map[String, DataFrame]]
        var dataLily = dataMap("Manufa")
        var dataMarket = dataMap("market")
        val mappingDF = dataMap("movMktOne")
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

        val filter_func_rmb: DataFrame => DataFrame = DataFrame => {
            DataFrame.filter(col("TYPE") === "LC-RMB")
        }
        val normal: phLyMOVData => Boolean = phLyMOVData => {
            true
        }
        val funcFileter = Map("LC-RMB" -> filter_func_rmb)

        dataLily = funcFileter(primaryValueName)(dataLily.filter(col("DATE") >= allTimelst.min && col("DATE") <= allTimelst.max))
        dataMarket = funcFileter(primaryValueName)(dataMarket.filter(col("DATE") >= allTimelst.min && col("DATE") <= allTimelst.max))
        dataMarket = dataMarket.join(mappingDF, dataMarket("ID") === mappingDF("ID")).select("DISPLAY_NAME", "DATE", "TYPE", "VALUE")
                        .withColumnRenamed("DISPLAY_NAME","ID")
        data = dataLily.union(dataMarket)

        val rddTemp = data.toJavaRDD.rdd.map(x => phLyMOVData(x(0).toString, x(1).toString, x(2).toString, BigDecimal(x(3).toString)))
        /**
          * 1. 整理所有需要的 display Name
          */
        val filter_display_name = rddTemp.filter(x => displayNamelList.contains(x.id))
                .filter(x => x.date >= allTimelst.min)
                .filter(x => x.date <= allTimelst.max)
                .filter(x => x.tp == primaryValueName)

        /**
          * 2. reduce by key 就是以display name 求和, 叫中间和
          */

        println("****************************************************")
        filter_display_name.take(20).foreach(println)

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
                    .map(x => {
                        x.result = x.value
                        x
                    })
                    .keyBy(x => (x.id, timeline))
                    .reduceByKey { (left, right) =>
                        left.result = left.result + right.result
                        left
                    }
        }.reduce((rdd1, rdd2) => rdd1.union(rdd2))

        println("............................................................")
        mid_sum.take(20).foreach(println)
        mid_sum.take(20).foreach(x=>println(x._2.result))

        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        val result: DataFrame = mid_sum.map(iter =>
            (iter._2.id, iter._1._2, iter._2.result)
        ).toDF("DISPLAY_NAME", "TIMELINE", "RESULT")
        result
    }
}
