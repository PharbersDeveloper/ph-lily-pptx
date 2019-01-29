package com.pharbers.process.stm.step.pptx.slider.content.city

import com.pharbers.process.common.{phCommand, phLyFactory, phLycalData}
import com.pharbers.process.stm.step.pptx.slider.content.phReportTableCol
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame

class phCityRank extends phCommand with phReportTableCol {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val cityData = argsMap("cityData").asInstanceOf[DataFrame]
        val countryData = argsMap("countryData").asInstanceOf[DataFrame]
        val timelineList = argsMap("timelineList").asInstanceOf[List[String]]
        val primaryValueName: String = argsMap("primaryValueName").asInstanceOf[String]
        val mktDisplayName: String = argsMap("mktDisplayName").asInstanceOf[String]
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
        val rddTemp = cityData.toJavaRDD.rdd.map(x => phLycalData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
            BigDecimal(x(6).toString), BigDecimal(x(7).toString), BigDecimal(x(5).toString), x(8).toString))
        val countryRddTemp = countryData.toJavaRDD.rdd.map(x => phLycalData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
            BigDecimal(x(5).toString), BigDecimal(x(6).toString), BigDecimal(x(7).toString), x(8).toString))
        val filter_func_dot: phLycalData => Boolean = phLycalData => {
            phLycalData.dot > 0
        }
        val filter_func_rmb: phLycalData => Boolean = phLycalData => {
            phLycalData.tp == "LC-RMB"
        }
        val funcFileter: Map[String, phLycalData => Boolean] = Map("rmb" -> filter_func_rmb, "dot" -> filter_func_dot)
        val filter_display_name = rddTemp.filter(x => x.display_name == mktDisplayName)
                .filter(x => x.date >= allTimelst.min)
                .filter(x => x.date <= allTimelst.max)
                .filter(x => funcFileter(primaryValueName)(x))
        val country_filter_display_name = countryRddTemp.filter(x => x.display_name == mktDisplayName)
                .filter(x => x.date >= allTimelst.min)
                .filter(x => x.date <= allTimelst.max)
                .filter(x => funcFileter(primaryValueName)(x))
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
        val country_mid_sum = timelineList.map { timeline =>
            val startYm: String = getStartYm(timeline)
            val ymMap = getTimeLineYm(timeline)
            val month = ymMap("month").toString.length match {
                case 1 => "0" + ymMap("month")
                case _ => ymMap("month")
            }
            val endYm: String = ymMap("year").toString + month
            country_filter_display_name.filter(x => x.date >= startYm)
                    .filter(x => x.date <= endYm)
                    .map(x => {
                        x.result = x.value
                        x
                    })
                    .keyBy(x => timeline)
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
        val mktDisplayNameSum = country_mid_sum.map(x => x._2.result).sum()
        val resultList = mid_sum.map(x => (x._2.id, x._1._2, (x._2.result / mktDisplayNameSum) * 100))
                .sortBy(x => -x._3)
        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        val resultDF = resultList.toDF("CITY", "TIMELINE", "SOM in " + mktDisplayName)
        resultDF
    }
}
