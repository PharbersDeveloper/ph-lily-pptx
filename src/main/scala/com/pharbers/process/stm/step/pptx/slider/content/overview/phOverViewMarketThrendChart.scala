package com.pharbers.process.stm.step.pptx.slider.content.overview

import com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol.{marketGrowthCol, marketSomCol}
import com.pharbers.process.stm.step.pptx.slider.content.{colArgs, phReportContentTrendsChart}
import org.apache.spark.sql.DataFrame

class phOverViewMarketThrendChart extends phReportContentTrendsChart{
    override def colValue(colArgs: colArgs): Any ={
        val colMap = Map(
            "DOT(Mn)" -> "dot",
            "MMU" -> "dot",
            "Tablet" -> "dot",
            "RMB" -> "rmb",
            "RMB(Mn)" -> "rmb",
            "DOT" -> "dot",
            "Mg(Mn)" -> "dot",
            "MG(Mn)" -> "dot",
            "RMB(Mn)" -> "rmb",
            "" -> "empty"
        )
        val dataMap = colArgs.data.asInstanceOf[Map[String, DataFrame]]
        val result: Any = new marketSomCol().exec(Map("data" -> dataMap("market"),  "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName,"dot"), "mktDisplayName" -> colArgs.mktDisplayName))
        result
    }
}
