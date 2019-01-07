package com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol

import com.pharbers.process.common.{phCommand, phLyMOVData}
import com.pharbers.process.stm.step.pptx.slider.content.phReportTableCol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class PieTableCol extends phCommand with phReportTableCol {
    override def exec(args: Any): DataFrame = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val data: DataFrame = argsMap("data").asInstanceOf[DataFrame]
        val displayNamelList: List[String] = argsMap("allDisplayNames").asInstanceOf[List[String]]
        val primaryValueName = argsMap("primaryValueName").asInstanceOf[String]
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
        val filter_func_rmb: DataFrame => DataFrame = DataFrame => {
            DataFrame.filter(col("TYPE") === "LC-RMB")
        }
        val filter_func_dot: DataFrame => DataFrame = DataFrame => {
            DataFrame.filter(col("TYPE") === "ST-CNT.UNIT")
        }
        val funcFilter = Map("LC-RMB" -> filter_func_rmb, "dot" -> filter_func_dot)

        funcFilter(primaryValueName)(data.filter(col("DATE") >= allTimelst.min && col("DATE") <= allTimelst.max))
                .select("ID", "VALUE")
                .withColumnRenamed("ID", "DISPLAY_NAME")
                .groupBy("DISPLAY_NAME").sum()
                .withColumnRenamed("sum(VALUE)", "RESULT")
    }
}


class PieTableCol2 extends phCommand with phReportTableCol {
    override def exec(args: Any): DataFrame = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        data = argsMap("data").asInstanceOf[DataFrame]
        val mappingDf: DataFrame = argsMap("mapping").asInstanceOf[DataFrame]
        val displayNamelList: List[String] = argsMap("allDisplayNames").asInstanceOf[List[String]]
        val primaryValueName = argsMap("primaryValueName").asInstanceOf[String]
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
        val filter_func_rmb: DataFrame => DataFrame = DataFrame => {
            DataFrame.filter(col("TYPE") === "LC-RMB")
        }
        val filter_func_dot: DataFrame => DataFrame = DataFrame => {
            DataFrame.filter(col("TYPE") === "ST-CNT.UNIT")
        }
        val funcFilter = Map("LC-RMB" -> filter_func_rmb, "dot" -> filter_func_dot)

        data = funcFilter(primaryValueName)(data.filter(col("DATE") >= allTimelst.min && col("DATE") <= allTimelst.max))
        data.join(mappingDf, data("ID") === mappingDf("ID"))
                .select("DISPLAY_NAME", "VALUE")
                .groupBy("DISPLAY_NAME").sum()
                .withColumnRenamed("sum(VALUE)", "RESULT")
    }
}