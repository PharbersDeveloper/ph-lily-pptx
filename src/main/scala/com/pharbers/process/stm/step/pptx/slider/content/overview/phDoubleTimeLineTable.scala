package com.pharbers.process.stm.step.pptx.slider.content.overview

import com.pharbers.process.stm.step.pptx.slider.content.{cell, phReportContentDotAndRmbTable, phReportContentTrendsTable, tableArgs}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class phDoubleTimeLineTable extends phReportContentTrendsTable {
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
