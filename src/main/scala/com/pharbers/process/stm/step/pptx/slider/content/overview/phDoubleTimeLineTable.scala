package com.pharbers.process.stm.step.pptx.slider.content.overview

import com.pharbers.process.stm.step.pptx.slider.content._
import com.pharbers.process.stm.step.pptx.slider.content.overview.col.lilyGroupGrowthCol
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class phDoubleTimeLineTable extends phReportContentTrendsTable {
    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        val timeList = colArgs.timelineList
        val dataList = timeList.map(x => {
            colArgs.timelineList = List(x)
            colValue(colArgs)
        })
        createTable(tableArgs, dataList)
    }

    override def colValue(colArgs: colArgs): Any = {
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
        val result: Any = new lilyGroupGrowthCol().exec(Map("data" -> colArgs.data, "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName,"dot"), "mktDisplayName" -> colArgs.mktDisplayName))
        result
    }


    override def putTableValue(data: Any, cellMap: Map[(String, String, String), (cell, String => String)]): List[cell] = {
        data.asInstanceOf[List[DataFrame]].foreach(rdd => {
            val resultMap = rdd.asInstanceOf[RDD[(String, List[String])]].collect().toMap
            cellMap.foreach(x => {
                x._2._1.value = resultMap.getOrElse(x._1._1,List.fill(24)("0"))(x._1._2.toInt)
            })
        })
        cellMap.values.map(x => x._1).toList
    }
}
