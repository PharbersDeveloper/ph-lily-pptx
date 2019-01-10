package com.pharbers.process.stm.step.pptx.slider.content.overview

import java.util.UUID

import com.pharbers.process.common.jsonData.{col, phTable, row}
import com.pharbers.process.common.phLyFactory
import com.pharbers.process.stm.step.pptx.slider.content._
import com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol.{marketGrowthCol, marketSomCol, movGetValue}
import com.pharbers.spark.phSparkDriver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import play.api.libs.json._

class phOverViewMarketTrendTable extends phReportContentTrendsTable {
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
        val result: Any = new marketGrowthCol().exec(Map("data" -> dataMap("market"),"mapping"-> dataMap("movMktOne"),  "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName,"dot"), "mktDisplayName" -> colArgs.mktDisplayName))
        result
    }

}

class phOverViewMarketTrendLine extends phReportContentTableBaseImpl {
    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        var data = colPrimaryValue(colArgs)
        data = colOtherValue(colArgs, data)
        createTable(tableArgs, data)
    }

    override def colPrimaryValue(colArgs: colArgs): DataFrame ={
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
            "RMB(Bn)" -> "LC-RMB",
            "" -> "empty"
        )
        val dataMap = colArgs.data.asInstanceOf[Map[String, DataFrame]]
        val dataDF = dataMap("market") union dataMap("Manufa")
        val mapDF = setMapDF(dataMap("movMktOne"))
        val data = dataDF.join(mapDF, dataDF("ID") === mapDF("ID")).select("DISPLAY_NAME","DATE", "TYPE", "VALUE")
        val result: Any = new movGetValue().exec(Map("data" -> data,  "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName,"dot"), "mktDisplayName" -> "all"))
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
                    cellMap = cellMap ++ Map((displayName, timeline, colName) -> (cell(jobid, tableName, valueCell, "", "Number", List(colCss, rowCss)), data2Value))
                }
            }
        }
        cellMap
    }

    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
        Thread.sleep(3000)
        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "LineNoTable")
    }

    def setMapDF(mapDf: DataFrame): DataFrame = {
        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        mapDf union List(("Lilly", "ELI LILLY GROUP")).toDF("DISPLAY_NAME", "ID")
    }
}

class phOverViewMarketTrendLineNoTable extends phOverViewMarketTrendTable {

    override def getColArgs(args: Any): colArgs = {
        val col2DataColMap = Map(
            "SOM(%)" -> "som",
            "SOM" -> "som",
            "SOM%" -> "som",
            "Share" -> "som",
            "Growth(%)" -> "Growth(%)",
            "GROWTH(%)" -> "Growth(%)",
            "YoY GR(%)" -> "Growth(%)",
            "YoY GR" -> "Growth(%)",
            "YOY GR" -> "Growth(%)",
            "GR(%)" -> "Growth(%)",
            "MAT Product Growth(%)" -> "Growth(%)",
            "YTD Product Growth(%)" -> "Growth(%)",
            "Rolling QTR Product Growth(%)" -> "Growth(%)",
            "MTH Product Growth(%)" -> "Growth(%)",
            "SOM in Branded MKT(%)" -> "som",
            "SOM in Branded MKT%" -> "som")
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val element = argsMap("element").asInstanceOf[JsValue]
        val rowList = (element \ "row" \ "display_name").as[List[String]].map(x => x.split(":").head.replace("%", ""))
        val colList = (element \ "col" \ "count").as[List[String]].map(x => col2DataColMap.getOrElse(x.split(":").head,x.split(":").head))
        val timelineList = (element \ "timeline").as[List[String]].map(x => x.split(":").head)
        val mktDisplayName = ((element \ "mkt_display").as[String] :: rowList.head :: Nil).filter(x => x != "").head
        val displayNameList = rowList.:::((element \ "col" \ "count").as[List[String]].map(x => x.split(":").head.split(" (in|of) ").tail.headOption.getOrElse(""))).::(mktDisplayName)
                .distinct.filter(x => x != "")
        val primaryValueName = ((element \ "mkt_col").as[String] :: colList.head :: Nil).filter(x => x != "").head
        val data = argsMap("data").asInstanceOf[Map[String, DataFrame]]((element \ "data").as[String])
        colArgs(rowList, colList, timelineList, displayNameList, mktDisplayName, primaryValueName, data)
    }

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
        val result: Any = new marketSomCol().exec(Map("data" -> colArgs.data, "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName,"dot"), "mktDisplayName" -> colArgs.mktDisplayName))
        result
    }

    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
        Thread.sleep(3000)
        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "LineNoTable")
    }
}

class phOverViewMarketSomTable extends phOverViewMarketTrendLine{
    override def colPrimaryValue(colArgs: colArgs): DataFrame ={
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
            "RMB(Bn)" -> "LC-RMB",
            "" -> "empty"
        )
        val dataMap = colArgs.data.asInstanceOf[Map[String, DataFrame]]
        val dataDF = dataMap("ManufaMNC") union dataMap("LLYProd")
        val mapDF = setMapDF(dataMap("movMktTwo"), colArgs.mktDisplayName)
        val data = dataDF.join(mapDF, dataDF("ID") === mapDF("ID")).select("DISPLAY_NAME","DATE", "TYPE", "VALUE")
        val result: Any = new movGetValue().exec(Map("data" -> data,  "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName,"dot"), "mktDisplayName" -> "all"))
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
            val timeToHead: String => String = time => {
                Map(
                    "" -> "Month", "RQ" -> "Rolling QTR", "MAT" -> "MAT", "YTD" -> "YTD"
                ).getOrElse(time.split(" ").head,"Month")
            }
            addCell(jobid, tableName, (timelineIndex + 1 + 65).toChar.toString + "1", timeToHead(timelineAndCss._1),
                "String", List(timelineAndCss._2))
        }
        addCell(jobid, tableName, "A1", colTitle._1 + timelineList.head._1.split(" ").tail.mkString(" "), "String", List(colTitle._2))

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
                    cellMap = cellMap ++ Map((displayName, timeline, colName) -> (cell(jobid, tableName, valueCell, "", "Number", List(colCss, rowCss)), data2Value))
                }
            }
        }
        cellMap
    }

    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
        Thread.sleep(3000)
        socketDriver.excel2PPT(jobid, tableName, pos, sliderIndex)
    }

    def setMapDF(mapDf: DataFrame, mktDisplayName: String): DataFrame = {
        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        mapDf union List((mktDisplayName, "ELI LILLY GROUP")).toDF("DISPLAY_NAME", "ID")
    }
}