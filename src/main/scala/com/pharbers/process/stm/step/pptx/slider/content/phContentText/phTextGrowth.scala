package com.pharbers.process.stm.step.pptx.slider.content.phContentText

import com.pharbers.process.common.phCommand
import com.pharbers.process.stm.step.pptx.slider.content.{growth, valueDF}
import org.apache.spark.sql.DataFrame

class phTextGrowth extends phCommand {
	override def exec(args: Any): Any = {
		val argsMap = args.asInstanceOf[Map[String, Any]]
		val timelineList = List(argsMap("timelineList").asInstanceOf[String])
		val nameList = argsMap("name").asInstanceOf[List[String]]
		val midDF: DataFrame = new valueDF().exec(args)
		val growthDF = new growth().exec(Map("timelineList" -> timelineList, "data" -> midDF)).asInstanceOf[DataFrame]
		val result = growthDF.collect().map(x => x.get(3).toString).head
		Map(nameList.head -> result)
	}
}
