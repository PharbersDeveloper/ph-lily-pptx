package com.pharbers.process.stm.step.pptx.slider.content.phContentText.overview

import com.pharbers.process.common.{phCommand, phLyFactory}
import com.pharbers.process.stm.step.pptx.slider.content.phReportTableCol
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class movRmbValue extends phCommand with phReportTableCol {
	override def exec(args: Any): Any = {
		val argsMap = args.asInstanceOf[Map[String, Any]]
		val data = argsMap("data").asInstanceOf[DataFrame]
		val timelineList: List[String] = argsMap("timelineList").asInstanceOf[List[String]]
		val primaryValueName: String = argsMap("primaryValueName").asInstanceOf[String]
		val valueType = argsMap("valueType").asInstanceOf[String]
		val valueId = argsMap("valueId").asInstanceOf[String]
		val allTimelineList: List[String] = timelineList
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

		val filter_func_rmb: DataFrame => DataFrame = dataFrame => {
			dataFrame.filter(col("TYPE") === "LC-RMB")
		}
		val funcFileter = Map("LC-RMB" -> filter_func_rmb)
		val filteredData = funcFileter(primaryValueName)(data.filter(col("DATE") >= allTimelst.min && col("DATE") <= allTimelst.max))

		lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
		import sparkDriver.ss.implicits._

		val func_total: String => Double = valueID => {
			filteredData.map(x => x.get(3).toString.toDouble).reduce(_ + _)
		}
		val func_company: String => Double = valueID => {
			filteredData.filter(col("ID") === valueID).map(x => x.get(3).toString.toDouble).reduce(_ + _)
		}
		val funcMap = Map("total" -> func_total, "company" -> func_company)
		val result = funcMap(valueType)(valueId)
		result
	}
}
