package com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol

import java.util.UUID

import com.pharbers.process.stm.step.pptx.slider.content._
import com.pharbers.process.stm.step.pptx.slider.content.overview.phOverViewRankColumnTable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import play.api.libs.json.JsValue

class phOverViewRankLineTable extends phOverViewRankColumnTable{
    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        colArgs.data = mapping(colArgs.data.asInstanceOf[DataFrame], colArgs.mapping.asInstanceOf[DataFrame])
        var data = colPrimaryValue(colArgs)
        data = getTopData(data, 10, colArgs.displayNameList)
        val displayNameList = getDisplayName(data)
        colArgs.displayNameList = displayNameList
        val dataRDD = colValue(colArgs)
        tableArgs.rowList = displayNameList.map(x => (x, "row_5"))
        createTable(tableArgs, dataRDD)
    }

    override def createTable(tableArgs: tableArgs, data: Any): Unit = {
        val cellMap = createTableStyle(tableArgs)
        val cellList = putTableValue(data, cellMap)
        pushTable(cellList, tableArgs.pos, tableArgs.slideIndex)
    }

    override def putTableValue(data: Any, cellMap: Map[(String, String, String), (cell, String => String)]): List[cell] = {
        val rdd = data.asInstanceOf[RDD[(String, List[String])]]
        val resultMap = rdd.collect().toMap
        cellMap.foreach(x => {
            x._2._1.value = resultMap.getOrElse(x._1._1,List.fill(24)("0"))(x._1._2.toInt)
        })
        cellMap.values.map(x => x._1).toList
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
        val mktDisplayName = tableArgs.mktDisplayName
        val col2dataColMap = tableArgs.col2DataColMap
        val common: String => String = x => x
        val mn: String => String = x => (x.toDouble / 1000000).toString
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
                    cellMap = cellMap ++ Map((displayName, ymIndex.toString, colName) -> (cell(jobid, tableName, valueCell, "", "Number", List(colCss, rowCss)), data2Value))
                }
            }
        }
        cellMap
    }

    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
        Thread.sleep(3000)
        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "Line")
    }

    def getDisplayName(dataFrame: DataFrame): List[String] ={
        val colName = dataFrame.columns
        val rows = dataFrame.collect()
        var displayNameList: List[String] = Nil
        rows.foreach(x => {
            val withName = x.toSeq.zip(colName).toList
            displayNameList = displayNameList :+ withName.find(x => x._2.equals("DISPLAY_NAME")).get._1.toString
        })
        displayNameList
    }

    def mapping(dataFrame: DataFrame, mappingDF: DataFrame): DataFrame ={
        val displayNameList = getDisplayName(mappingDF)
        dataFrame.filter(col("ID").isin(displayNameList: _*))
    }
}
