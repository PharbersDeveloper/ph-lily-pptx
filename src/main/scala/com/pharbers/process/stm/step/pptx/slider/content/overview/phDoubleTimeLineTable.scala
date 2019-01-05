package com.pharbers.process.stm.step.pptx.slider.content.overview

import java.util.UUID

import com.pharbers.process.stm.step.pptx.slider.content._
import com.pharbers.process.stm.step.pptx.slider.content.overview.col.lilyGroupGrowthCol
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class phDoubleTimeLineTable extends phReportContentDotAndRmbTable {
    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        val dataMap = colArgs.data.asInstanceOf[Map[String, DataFrame]]
        val timeList = colArgs.timelineList
        val dataList = timeList.map(x => {
//            colArgs.data = dataMap("Manufa")
            colArgs.timelineList = List(x)
            val data = colPrimaryValue(colArgs)
            colOtherValue(colArgs,data)
        })
        createTable(tableArgs, dataList)
    }
    override def colPrimaryValue(colArgs: colArgs): DataFrame = {
        val colMap = Map(
            "DOT(Mn)" -> "dot",
            "MMU" -> "dot",
            "Tablet" -> "dot",
            "RMB" -> "LC-RMB",
            "RMB(Mn)" -> "LC-RMB",
            "DOT" -> "dot",
            "Mg(Mn)" -> "dot",
            "MG(Mn)" -> "dot",
            "RMB(Mn)" -> "LC-RMB",
            "" -> "empty"
        )
        val rowMap = Map("Lilly" -> "ELI LILLY GROUP")
        val allDisplayNames =  colArgs.displayNameList.map( x => rowMap.getOrElse(x,x))
        val result = new lilyGroupGrowthCol().exec(Map("data" -> colArgs.data, "allDisplayNames" -> allDisplayNames, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName,"dot"), "mktDisplayName" -> colArgs.mktDisplayName))
        result.asInstanceOf[DataFrame]
    }

    override def createTableStyle(tableArgs: tableArgs): Map[(String, String, String), (cell, String => String)] = {
        var cellMap: Map[(String, String, String),  (cell, String => String)] = Map()
        val tableName = UUID.randomUUID().toString
        val rowTitle = tableArgs.rowTitle
        val colTitle = tableArgs.colTitle
        val rowList = tableArgs.rowList
        val colList = tableArgs.colList
        val timelineList = tableArgs.timelineList
        val jobid = tableArgs.jobid
        val col2dataColMap = tableArgs.col2DataColMap
        val common: String => String = x => x
        val mn: String => String = x => (x.toDouble / 1000000).toString
        val row2DataMap = Map("Lilly" -> "ELI LILLY GROUP")
        val data2ValueMap = Map("DOT(Mn)" -> mn,
            "MMU" -> common,
            "Tablet" -> common,
            "RMB" -> common,
            "RMB(Mn)" -> mn,
            "DOT" -> common,
            "Mg(Mn)" -> mn,
            "MG(Mn)" -> mn,
            "RMB(Mn)" -> mn)
        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
            val timeline = timelineAndCss._1
            val timelineCss = timelineAndCss._2
            val timeLineCell = (timelineIndex + 1 + 65).toChar.toString + "1"
            addCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
        }
        (rowTitle :: Nil).zipWithIndex.foreach {
            case (titleANdCss, index) =>
                val title = titleANdCss._1
                val css = titleANdCss._2
                val colCell = "A" + (index + 1)
                addCell(jobid, tableName, colCell, title, "String", List(css, colTitle._1))
        }

        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowCss = displayNameAndCss._2
            val displayName = displayNameAndCss._1
            val rowIndex = displayNameIndex + 2
            val displayCell = "A" + (displayNameIndex + 2).toString
            addCell(jobid, tableName, displayCell, displayName, "String", List(rowTitle._2, rowCss))
            timelineList.zipWithIndex.foreach { case (timelineAndCss, ymIndex) =>
                val timeline = timelineAndCss._1
                val colIndex = ymIndex + 1
                colList.foreach { colNameAndCss =>
                    val colName = col2dataColMap.getOrElse(colNameAndCss._1, colNameAndCss._1)
                    val data2Value = data2ValueMap.getOrElse(colNameAndCss._1,common)
                    val colCss = colNameAndCss._2
                    val valueCell = (colIndex + 65).toChar.toString + rowIndex.toString
                    cellMap = cellMap ++ Map((row2DataMap.getOrElse(displayName, displayName), timeline, colName) -> (cell(jobid, tableName, valueCell, "", "Number", List(colCss, rowCss)), data2Value))
                }
            }
        }
        cellMap
    }


    override def putTableValue(dataFrameList: List[DataFrame], cellMap: Map[(String, String, String), (cell, String => String)]): List[cell] = {
        dataFrameList.zip(List("DOT ","RMB ")).foreach(data => {
            val dataFrame = data._1
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
