package com.pharbers.process.stm.step.pptx.slider.content.overview

import com.pharbers.process.stm.step.pptx.slider.content.{cell, phReportContentDotAndRmbTable, phReportContentTrendsTable, tableArgs}
import org.apache.spark.sql.DataFrame

class phDoubleTimeLineTable extends phReportContentDotAndRmbTable{
    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        val timeList = colArgs.timelineList
        val dataList = timeList.map(x => {
            colArgs.timelineList = List(x)
            colOtherValue(colArgs, colPrimaryValue(colArgs))
        })
        createTable(tableArgs, dataList)
    }

    override def createTableStyle(tableArgs: tableArgs): Map[(String, String, String), (cell, String => String)] = {
        new phReportContentTrendsTable().createTableStyle(tableArgs)
    }

    override def putTableValue(dataFrameList: List[DataFrame], cellMap: Map[(String, String, String), (cell, String => String)]): List[cell] = {
        dataFrameList.foreach(data => {
            val dataFrame = data
            val common: String => String = x => x
            val dataColNames = dataFrame.columns
            dataFrame.collect().foreach(x => {
                val row = x.toSeq.zip(dataColNames).toList
                val displayName = row.find(x => x._2.equals("DISPLAY_NAME")).get._1.toString
                val timeline = row.find(x => x._2.equals("TIMELINE")).get._1.toString
                row.foreach(x => {
                    val oneCell = cellMap.getOrElse((displayName, timeline, x._2),(cell("","","","","",Nil),common))
                    oneCell._1.value = oneCell._2(x._1.toString)
                })
            })
        })
        cellMap.values.map(x => x._1).toList
    }
}
