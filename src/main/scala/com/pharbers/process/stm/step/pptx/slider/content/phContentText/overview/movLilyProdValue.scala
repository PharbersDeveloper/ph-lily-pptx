package com.pharbers.process.stm.step.pptx.slider.content.phContentText.overview

import com.pharbers.process.common.phCommand
import org.apache.spark.sql.DataFrame

class movLilyProdValue extends phCommand {
	override def exec(args: Any): Any = {
		val argsMap = args.asInstanceOf[Map[String, Any]]
		val timelineList: List[String] = List(argsMap("timeline").asInstanceOf[String])
		val data: DataFrame = argsMap("data").asInstanceOf[DataFrame]
		val resultNameList: List[String] = argsMap("name").asInstanceOf[List[String]]
		val c = new movRmbValue()
		val totalValue = c.exec(Map("data" -> data, "colList" -> "RMB", "timelineList" -> timelineList,
			"primaryValueName" -> "LC-RMB", "valueType" -> "total", "valueId" -> "total")).asInstanceOf[Double]
		Map(resultNameList.head -> totalValue)
	}
}