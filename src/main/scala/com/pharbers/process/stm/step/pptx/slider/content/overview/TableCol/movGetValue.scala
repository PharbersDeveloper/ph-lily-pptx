package com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol

import com.pharbers.process.common.{phCommand, phLyFactory, phLyMOVData}
import com.pharbers.process.stm.step.pptx.slider.content.phReportTableCol
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class movGetValue extends phCommand with phReportTableCol {
    override def exec(args: Any): DataFrame = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val data = argsMap("data").asInstanceOf[DataFrame]
        val displayNamelList: List[String] = argsMap("allDisplayNames").asInstanceOf[List[String]]
        val timelineList = argsMap("timelineList").asInstanceOf[List[String]]
        val colList: List[String] = argsMap("colList").asInstanceOf[List[String]]
        val mktDisplayName: String = argsMap("mktDisplayName").asInstanceOf[String]
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
        val rddTemp = data.toJavaRDD.rdd.map(x => phLyMOVData(x(0).toString, x(1).toString, x(2).toString,
            BigDecimal(x(3).toString)))
        val filter_TimeLine = rddTemp.filter(x => x.date >= allTimelst.min)
            .filter(x => x.date <= allTimelst.max)
            .filter(x => x.tp == primaryValueName)
        val mid_sum = allTimelineList.map { timeline =>
            val startYm: String = getStartYm(timeline)
            val ymMap = getTimeLineYm(timeline)
            val month = ymMap("month").toString.length match {
                case 1 => "0" + ymMap("month")
                case _ => ymMap("month")
            }
            val endYm: String = ymMap("year").toString + month
            filter_TimeLine.filter(x => x.date >= startYm)
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

        val mktValue = mid_sum.keyBy(x => x._1._2)
                .reduceByKey{ (left, right) =>
                    left._2.result = left._2.result + right._2.result
                    left
                }.map(x => (mktDisplayName, x._1, x._2._2.result))

        val filter_display_name = mid_sum.filter(x => displayNamelList.contains(x._1._1))
            .map(x => (x._1._1, x._1._2, x._2.result))

        val resultRDD = mktValue.union(filter_display_name)

        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        val result: DataFrame = resultRDD.toDF("DISPLAY_NAME", "TIMELINE", "RESULT")
        result
    }
}
