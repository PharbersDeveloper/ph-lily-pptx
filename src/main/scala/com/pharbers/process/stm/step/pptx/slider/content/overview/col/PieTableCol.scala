package com.pharbers.process.stm.step.pptx.slider.content.overview.col

import com.pharbers.process.common.phCommand
import com.pharbers.process.stm.step.pptx.slider.content.phReportTableCol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class PieTableCol extends phCommand with phReportTableCol {
    override def exec(args: Any): DataFrame = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val data: DataFrame = argsMap("data").asInstanceOf[DataFrame]
        val displayNamelList: List[String] = argsMap("allDisplayNames").asInstanceOf[List[String]]
        val colList: List[String] = argsMap("colList").asInstanceOf[List[String]]
        val timelineList: List[String] = argsMap("timelineList").asInstanceOf[List[String]]
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

        data.filter(col("DATE") >= allTimelst.min && col("DATE") <= allTimelst.max)
                .select("MANUF", "VALUE")
                .withColumnRenamed("VALUE", "RESULT")
                .withColumnRenamed("MANUF", "DISPLAY_NAME")
                .groupBy("DISPLAY_NAME").sum()
                .withColumnRenamed("sum(RESULT)", "RESULT")
    }
}
