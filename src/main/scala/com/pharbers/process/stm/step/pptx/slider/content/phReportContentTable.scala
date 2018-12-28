package com.pharbers.process.stm.step.pptx.slider.content

import java.util.UUID

import com.pharbers.phsocket.phSocketDriver
import com.pharbers.process.common.{phCommand, phLyCalData, phLyFactory}
import com.pharbers.spark.phSparkDriver
import org.apache.poi.xslf.usermodel.XSLFSlide
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

import scala.collection.mutable

object phReportContentTable {
    val functionMap = Map(
        "DOT(Mn)" -> "dotMn",
        "MMU" -> "dot",
        "Tablet" -> "tablet",
        "SOM(%)" -> "som",
        "Growth(%)" -> "GrowthPercentage",
        "YoY GR(%)" -> "GrowthPercentage",
        "GR(%)" -> "GrowthPercentage",
        "RMB" -> "rmb",
        "RMB(Mn)" -> "rmbMn",
        "DOT" -> "dot",
        "SOM in Branded MKT(%)" -> "som",
        "Mg(Mn)" -> "dotMn",
        "MG(Mn)" -> "dotMn",
        "RMB(Mn)" -> "rmbMn",
        "" -> "empty"
    )

    def colName2FunctionName(name: String): String = {
        functionMap.getOrElse(name, throw new Exception("未定义方法" + name))
    }
}

trait phReportContentTable extends phCommand{
    val socketDriver = phSocketDriver()

    def pushCell(jobid: String, tableName: String, cell: String, value: String, cate: String, cssName: List[String]): Unit =
        socketDriver.setExcel(jobid, tableName, cell, value, cate, cssName)


    def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit =
        socketDriver.excel2PPT(jobid, tableName, pos, sliderIndex)

    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        colPrimaryValue(colArgs)
        val data = colOtherValue(colArgs)
        createTable(tableArgs, data)
    }

    def getColArgs(args: Any) : colArgs

    def getTableArgs(args: Any) : tableArgs

    def colPrimaryValue(colArgs: colArgs) : DataFrame

    def colOtherValue(colArgs: colArgs): DataFrame

    def createTable(tableArgs: tableArgs, data: DataFrame): Unit


    case class colArgs(rowList: List[String], colList: List[String], timelineList: List[String], displayNameList: List[String],
                       mktDisplayName: String, primaryValueName: String, data: DataFrame)

    case class tableArgs(rowList: List[(String, String)], colList: List[(String, String)], timelineList: List[(String, String)], mktDisplayName: String,
                         jobid: String, pos: List[Int], colTitle: (String, String), rowTitle: (String, String), slideIndex: Int)

    case class cell(jobid: String, tableName: String, cell: String, var value: String, cate: String, cssName: List[String])
}

class phReportContentTableBaseImpl extends  phReportContentTable {

    override def getColArgs(args: Any): colArgs = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val element = argsMap("element").asInstanceOf[JsValue]
        val rowList = (element \ "row" \ "display_name").as[List[String]].map(x => x.split(":").head)
        val colList = (element \ "col" \ "count").as[List[String]].map(x => x.split(":").head.split(" (in|of) ").head)
        val timelineList = (element \ "timeline").as[List[String]].map(x => x.split(":").head)
        val mktDisplayName = ((element \ "mkt_display").as[String] :: rowList.head :: Nil).filter(x => x != "").head
        val displayNameList = rowList.:::((element \ "col" \ "count").as[List[String]].map(x => x.split(":").head.split(" (in|of) ").tail.headOption.getOrElse(""))).::(mktDisplayName)
          .distinct.filter(x => x != "")
        val primaryValueName = ((element \ "mkt_col").as[String] :: colList.head :: Nil).filter(x => x != "").head
        val data = argsMap("data").asInstanceOf[DataFrame]
        colArgs(rowList, colList, timelineList, displayNameList, mktDisplayName, primaryValueName, data)
    }

    override def getTableArgs(args: Any): tableArgs = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val jobid = args("jobid")
        val slideIndex = args("slideIndex")
        val element = argsMap("element").asInstanceOf[JsValue]
        val rowList = (element \ "row" \ "display_name").as[List[String]].map(x => (x.split(":").head, x.split(":").tail.headOption.getOrElse("")))
        val colList = (element \ "col" \ "count").as[List[String]].map(x => (x.split(":").head, x.split(":").tail.headOption.getOrElse("")))
        val timelineList = (element \ "timeLine").as[List[String]].map(x => (x.split(":").head, x.split(":").tail.headOption.getOrElse("")))
        val pos = (element \ "pos").as[List[Int]]
        val colTitle = ((element \ "col" \ "title").as[String].split(":").head, (element \ "col" \ "title").as[String].split(":").tail.headOption.getOrElse(""))
        val rowTitle = ((element \ "row" \ "title").as[String].split(":").head, (element \ "row" \ "title").as[String].split(":").tail.headOption.getOrElse(""))
        val mktDisplayName: String = ((element \ "mkt_display").as[String] :: rowList.head._1 :: Nil).filter(x => x != "").head
        tableArgs(rowList, colList, timelineList,mktDisplayName, jobid, pos, colTitle, rowTitle, slideIndex)
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
        new valueDf.exec(Map("data" -> colArgs.data, "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName, colArgs.primaryValueName)))
    }

    override def colOtherValue(colArgs: colArgs): DataFrame = {
        val rowList = colArgs.rowList
        val colList = colArgs.colList
        val timelineList = colArgs.timelineList
        val mktDisplayName = colArgs.mktDisplayName
        val somCommand: phCommand = null
        val growthCommand: phCommand = null
        val empty: phCommand = new phCommand {
            override def exec(args: Any): Any = null
        }
        val colMap = Map("som" -> somCommand, "growth" -> growthCommand)
        colList.map(x => {
            val mktDisplay =  x.split(" (in|of) ").tail.headOption.getOrElse(mktDisplayName)
            colMap.getOrElse(x.split(" (in|of) ").head, empty).exec(???)
        }).last.asInstanceOf[DataFrame]
    }

    override def createTable(tableArgs: tableArgs, data: DataFrame): Unit = {
        val cellMap = createTableStyle(tableArgs)
        val cellList = putTableValue(data, cellMap)
        pushTable(cellList, tableArgs.pos, tableArgs.slideIndex)
    }

    def createTableStyle(tableArgs: tableArgs): Map[(String, String), cell] = {
        var cellMap: Map[(String, String), cell] = Map()
        val tableName = UUID.randomUUID().toString
        val rowTitle = tableArgs.rowTitle
        val colTitle = tableArgs.colTitle
        val rowList = tableArgs.rowList
        val colList = tableArgs.colList
        val timelineList = tableArgs.timelineList
        val jobid = tableArgs.jobid
        val mktDisplayName = tableArgs.mktDisplayName
        val colMap = Map("SOM" -> ("SOM in " + mktDisplayName))
        (rowTitle :: colTitle :: Nil).zipWithIndex.foreach{
            case (titleANdCss, index) =>
                val title = titleANdCss._1
                val css = titleANdCss._2
                val colCell = "A" + (index + 1)
                pushCell(jobid, tableName, colCell, title, "String", List(colTitle._2, css))
        }

        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
            val timeline = timelineAndCss._1
            val timelineCss = timelineAndCss._2
            val cellLeft = (1 + timelineIndex * colList.size + 65).toChar.toString + "1"
            val cellRight = (timelineIndex * colList.size + colList.size + 65).toChar.toString + "1"
            val timeLineCell = cellLeft + ":" + cellRight
            pushCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
            colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                val colName = colNameAndCss._1
                val colCss = colNameAndCss._2
                val colCell = (colList.size * timelineIndex + colNameIndex + 1 + 65).toChar.toString + "2"
                pushCell(jobid, tableName, colCell, colName, "String", List(colTitle._2, colCss))
            }
        }

        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowIndex = displayNameIndex + 3
            val rowCss = displayNameAndCss._2
            val displayName = displayNameAndCss._1
            pushCell(jobid, tableName, "A" + rowIndex.toString, displayName, "String", List(rowCss, rowTitle._2))

            timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
                val timeline = timelineAndCss._1
                val timelineCss = timelineAndCss._2
                colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                    val colName = colMap.getOrElse(colNameAndCss._1, colNameAndCss._1)
                    val colCss = colNameAndCss._2
                    val colIndex = colList.size * timelineIndex + colNameIndex + 1
                    val cellIndex = (colIndex + 65).toChar.toString + rowIndex.toString
                    cellMap = cellMap ++ Map((displayName + timeline, colName) -> cell(jobid, tableName, cellIndex, "", "Number", List(rowCss, colCss)))
                }
            }
        }
        cellMap
    }

    def putTableValue(dataFrame: DataFrame, cellMap: Map[(String, String) , cell]): List[cell] = {
        cellMap.map(x => {
            val displayName = x._1._1
            val colName = x._1._2
            x._2.value = dataFrame.select(displayName, colName).collectAsList().get(0).toString().replaceAll("[\\[\\]]","")
            x._2
        }).toList
    }

    def pushTable(cellList: List[cell], pos: List[Int], slideIndex: Int): Unit = {
        cellList.foreach(x => pushCell(x.jobid, x.tableName, x.cell, x.value, x.cate, x.cssName))
        pushExcel(cellList.head.jobid,cellList.head.tableName, pos, slideIndex)
    }
}

class phReportContentTableImpl extends phReportContentTableBaseImpl {

}

class phReportContentTrendsTable extends phReportContentTableBaseImpl {

    override def createTableStyle(tableArgs: tableArgs): Map[(String, String), cell] = {
        var cellMap: Map[(String, String), cell] = Map()
        val tableName = UUID.randomUUID().toString
        val rowTitle = tableArgs.rowTitle
        val colTitle = tableArgs.colTitle
        val rowList = tableArgs.rowList
        val colList = tableArgs.colList
        val timelineList = tableArgs.timelineList
        val jobid = tableArgs.jobid
        val mktDisplayName = tableArgs.mktDisplayName
        val colMap = Map("SOM" -> ("SOM in " + mktDisplayName))
        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
            val timeline = timelineAndCss._1
            val timelineCss = timelineAndCss._2
            val timeLineCell = (timelineIndex + 1 + 65).toChar.toString + "1"
            pushCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
        }
        (rowTitle :: Nil).zipWithIndex.foreach{
            case (titleANdCss, index) =>
                val title = titleANdCss._1
                val css = titleANdCss._2
                val colCell = "A" + (index + 1)
                pushCell(jobid, tableName, colCell, title, "String", List(colTitle._1, css))
        }

        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowCss = displayNameAndCss._2
            val displayName = displayNameAndCss._1
            val rowIndex = displayNameIndex + 2
            val displayCell = "A" + (displayNameIndex + 2).toString
            pushCell(jobid, tableName, displayCell, displayName, "String", List(rowCss, rowTitle._2))
            timelineList.zipWithIndex.foreach { case (timelineAndCss, ymIndex) =>
                val timeline = timelineAndCss._1
                val colIndex = ymIndex + 1
                colList.foreach { colNameAndCss =>
                    val colName = colMap.getOrElse(colNameAndCss._1, colNameAndCss._1)
                    val colCss = colNameAndCss._2
                    val valueCell = (colIndex + 65).toChar.toString + rowIndex.toString
                    cellMap = cellMap ++ Map((displayName + timeline, colName) -> cell(jobid, tableName, valueCell, "", "Number", List(rowCss, colCss)))
                }
            }
        }
        cellMap
    }
}

class phReportContentTrendsChart extends phReportContentTrendsTable {
    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "Line")
//        Unit
    }
}

class phReportContentComboChart extends phReportContentTrendsTable {
    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "Combo")
//        Unit
    }
}

class phReportContentOnlyLineChart extends phReportContentTrendsTable {
    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "LineNoTable")
//        Unit
    }
}

class phReportContentBlueGrowthTable extends phReportContentTrendsTable  {

    override def createTableStyle(tableArgs: tableArgs): Map[(String, String), cell] = {
        var cellMap: Map[(String, String), cell] = Map()
        val tableName = UUID.randomUUID().toString
        val rowTitle = tableArgs.rowTitle
        val colTitle = tableArgs.colTitle
        val rowList = tableArgs.rowList
        val colList = tableArgs.colList
        val timelineList = tableArgs.timelineList
        val jobid = tableArgs.jobid
        val mktDisplayName = tableArgs.mktDisplayName
        val colMap = Map("SOM" -> ("SOM in " + mktDisplayName))
        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
            val timeline = timelineAndCss._1
            val timelineCss = timelineAndCss._2
            val timeLineCell = getCellCoordinate(timelineIndex + 1, 1)
            pushCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
        }
        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowCss = displayNameAndCss._2
            val displayNameTemp = displayNameAndCss._1
            val displayName = displayNameTemp.replaceAll("%","")
            val rowIndex = displayNameIndex + 2
            val displayCell = "A" + (displayNameIndex + 1).toString + ":" + "A" + (displayNameIndex + 4).toString
            pushCell(jobid, tableName, displayCell, displayNameTemp, "String", List(rowCss, rowTitle._2))
            timelineList.zipWithIndex.foreach { case (timelineAndCss, ymIndex) =>
                val timeline = timelineAndCss._1
                val colIndex = ymIndex + 1
                colList.foreach { colNameAndCss =>
                    val colName = colMap.getOrElse(colNameAndCss._1, colNameAndCss._1)
                    val colCss = colNameAndCss._2
                    val valueCell = getCellCoordinate(colIndex, rowIndex)
                    cellMap = cellMap ++ Map((displayName + timeline, colName) -> cell(jobid, tableName, valueCell, "", "Number", List(rowCss, colCss)))
                }
            }
        }
        cellMap
    }
    def getCellCoordinate(colIndex: Int, rowIndex: Int): String ={
        if (colIndex <= 12){
            (colIndex + 65).toChar.toString + rowIndex.toString
        }else {
            (colIndex + 53).toChar.toString + (rowIndex + 2).toString
        }
    }
}
