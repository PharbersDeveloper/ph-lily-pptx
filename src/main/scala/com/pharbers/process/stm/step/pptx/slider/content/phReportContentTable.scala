package com.pharbers.process.stm.step.pptx.slider.content

import java.util.UUID

import com.pharbers.phsocket.phSocketDriver
import com.pharbers.process.common.{phCommand, phLycalArray, phLycalData}
import com.pharbers.spark.phSparkDriver
import org.apache.poi.xslf.usermodel.XSLFSlide
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import org.apache.spark.sql.functions.col

import scala.collection.mutable

object phReportContentTable {

}

trait phReportContentTable extends phCommand {
    val socketDriver = phSocketDriver()
    var cells: List[String] = Nil
    def addCell(jobid: String, tableName: String, cell: String, value: String, cate: String, cssName: List[String]): Unit ={
        val css = cssName.mkString("*")
        cells = cells :+ s"-c#$cell-s#$css-t#$cate-v#$value"
    }

    def pushCell(jobid: String, tableName: String): Unit ={
        cells.sliding(30, 30).foreach(x => {
            socketDriver.setExcel(jobid, tableName, x)
        })
    }

    def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit ={
        Thread.sleep(3000)
        socketDriver.excel2PPT(jobid, tableName, pos, sliderIndex)
    }


    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        var data = colPrimaryValue(colArgs)
        data = colOtherValue(colArgs, data)
        createTable(tableArgs, data)
    }

    def getColArgs(args: Any): colArgs

    def getTableArgs(args: Any): tableArgs

    def colPrimaryValue(colArgs: colArgs): DataFrame

    def colOtherValue(colArgs: colArgs, data: DataFrame): DataFrame

    def createTable(tableArgs: tableArgs, data: DataFrame): Unit


    case class colArgs(rowList: List[String], colList: List[String], var timelineList: List[String], displayNameList: List[String],
                       mktDisplayName: String, var primaryValueName: String, data: DataFrame)

    case class tableArgs(rowList: List[(String, String)], colList: List[(String, String)], timelineList: List[(String, String)], mktDisplayName: String,
                         jobid: String, pos: List[Int], colTitle: (String, String), rowTitle: (String, String), slideIndex: Int, col2DataColMap: Map[String, String])

    case class cell(jobid: String, tableName: String, cell: String, var value: String, cate: String, cssName: List[String])

}

class phReportContentTableBaseImpl extends phReportContentTable {

    override def getColArgs(args: Any): colArgs = {
        val col2DataColMap = Map(
            "SOM(%)" -> "som",
            "SOM" -> "som",
            "SOM%" -> "som",
            "Share" -> "som",
            "Growth(%)" -> "Growth(%)",
            "YoY GR(%)" -> "Growth(%)",
            "YoY GR" -> "Growth(%)",
            "YOY GR" -> "Growth(%)",
            "GR(%)" -> "Growth(%)",
            "SOM in Branded MKT(%)" -> "som")
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val element = argsMap("element").asInstanceOf[JsValue]
        val rowList = (element \ "row" \ "display_name").as[List[String]].map(x => x.split(":").head.replace("%", ""))
        val colList = (element \ "col" \ "count").as[List[String]].map(x => col2DataColMap.getOrElse(x.split(":").head,x.split(":").head))
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
        val jobid = argsMap("jobid").asInstanceOf[String]
        val slideIndex = argsMap("slideIndex").asInstanceOf[Int]
        val element = argsMap("element").asInstanceOf[JsValue]
        val rowList = (element \ "row" \ "display_name").as[List[String]].map(x => (x.split(":").head, x.split(":").tail.headOption.getOrElse("")))
        val colList = (element \ "col" \ "count").as[List[String]].map(x => (x.split(":").head, x.split(":").tail.headOption.getOrElse("")))
        val timelineList = (element \ "timeline").as[List[String]].map(x => (x.split(":").head, x.split(":").tail.headOption.getOrElse("")))
        val pos = (element \ "pos").as[List[Int]]
        val colTitle = ((element \ "col" \ "title").as[String].split(":").head, (element \ "col" \ "title").as[String].split(":").tail.headOption.getOrElse(""))
        val rowTitle = ((element \ "row" \ "title").as[String].split(":").head, (element \ "row" \ "title").as[String].split(":").tail.headOption.getOrElse(""))
        val mktDisplayName: String = ((element \ "mkt_display").as[String] :: rowList.head._1 :: Nil).filter(x => x != "").head
        val col2DataColMap = Map("DOT(Mn)" -> "RESULT",
            "MMU" -> "RESULT",
            "Tablet" -> "RESULT",
            "SOM(%)" -> ("SOM in " + mktDisplayName),
            "SOM" -> ("SOM in " + mktDisplayName),
            "SOM%" -> ("SOM in " + mktDisplayName),
            "Share" -> ("SOM in " + mktDisplayName),
            "Growth(%)" -> "GROWTH",
            "YoY GR(%)" -> "GROWTH",
            "YoY GR" -> "GROWTH",
            "YOY GR" -> "GROWTH",
            "GR(%)" -> "GROWTH",
            "RMB" -> "RESULT",
            "RMB(Mn)" -> "RESULT",
            "DOT" -> "RESULT",
            "SOM in Branded MKT(%)" -> ("SOM in " + mktDisplayName),
            "Mg(Mn)" -> "RESULT",
            "MG(Mn)" -> "RESULT",
            "RMB(Mn)" -> "RESULT")
        tableArgs(rowList, colList, timelineList, mktDisplayName, jobid, pos, colTitle, rowTitle, slideIndex,col2DataColMap)
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
        val result: DataFrame = new valueDF().exec(Map("data" -> colArgs.data, "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName, colArgs.primaryValueName)))
        result
    }

    override def colOtherValue(colArgs: colArgs, data: DataFrame): DataFrame = {
        val rowList = colArgs.rowList
        val colList = colArgs.colList.sorted
        val timelineList = colArgs.timelineList
        val mktDisplayName = colArgs.mktDisplayName
        val somCommand: phCommand = new som
        val growthCommand: phCommand = new growth
        val empty: phCommand = new phCommand {
            override def exec(args: Any): Any = args.asInstanceOf[Map[String, Any]]("data")
        }
        var dataFrame = data
        val colMap = Map(
            "SOM(%)" -> somCommand,
            "SOM" -> somCommand,
            "SOM%" -> somCommand,
            "Growth(%)" -> growthCommand,
            "YoY GR(%)" -> growthCommand,
            "GR(%)" -> growthCommand,
            "SOM in Branded MKT(%)" -> somCommand,
            "Share" -> somCommand
            )
        colList.foreach(x => {
            val mktDisplay = x.split(" (in|of) ").tail.headOption.getOrElse(mktDisplayName)
            dataFrame = colMap.getOrElse(x.split(" (in|of) ").head, empty).exec(Map(
                "data" -> dataFrame, "mktDisplayName" -> mktDisplay, "timelineList" -> timelineList
            )).asInstanceOf[DataFrame]
        })
        dataFrame
    }

    override def createTable(tableArgs: tableArgs, data: DataFrame): Unit = {
        val cellMap = createTableStyle(tableArgs)
        val cellList = putTableValue(data, cellMap)
        pushTable(cellList, tableArgs.pos, tableArgs.slideIndex)
    }

    def createTableStyle(tableArgs: tableArgs): Map[(String, String, String), (cell, String => String)] = {
        var cellMap: Map[(String, String, String), (cell, String => String)] = Map()
        val tableName = UUID.randomUUID().toString
        val rowTitle = tableArgs.rowTitle
        val colTitle = tableArgs.colTitle
        val rowList = tableArgs.rowList
        val colList = tableArgs.colList
        val timelineList = tableArgs.timelineList
        val jobid = tableArgs.jobid
        val col2DataColMap = tableArgs.col2DataColMap
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
        (rowTitle :: colTitle :: Nil).zipWithIndex.foreach {
            case (titleANdCss, index) =>
                val title = titleANdCss._1
                val css = titleANdCss._2
                val colCell = "A" + (index + 1)
                addCell(jobid, tableName, colCell, title, "String", List(css, colTitle._2))
        }

        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
            val timeline = timelineAndCss._1
            val timelineCss = timelineAndCss._2
            val cellLeft = (1 + timelineIndex * colList.size + 65).toChar.toString + "1"
            val cellRight = (timelineIndex * colList.size + colList.size + 65).toChar.toString + "1"
            val timeLineCell = cellLeft + ":" + cellRight
            addCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
            colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                val colName = colNameAndCss._1
                val colCss = colNameAndCss._2
                val colCell = (colList.size * timelineIndex + colNameIndex + 1 + 65).toChar.toString + "2"
                addCell(jobid, tableName, colCell, colName, "String", List(colCss, colTitle._2))
            }
        }

        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowIndex = displayNameIndex + 3
            val rowCss = displayNameAndCss._2
            val displayName = displayNameAndCss._1
            addCell(jobid, tableName, "A" + rowIndex.toString, displayName, "String", List(rowTitle._2, rowCss))

            timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
                val timeline = timelineAndCss._1
                val timelineCss = timelineAndCss._2
                colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                    val colName = col2DataColMap.getOrElse(colNameAndCss._1, colNameAndCss._1).replace("Share of", "SOM in")
                    val data2Value = data2ValueMap.getOrElse(colNameAndCss._1, common)
                    val colCss = colNameAndCss._2
                    val colIndex = colList.size * timelineIndex + colNameIndex + 1
                    val cellIndex = (colIndex + 65).toChar.toString + rowIndex.toString
                    cellMap = cellMap ++ Map((displayName, timeline, colName) -> (cell(jobid, tableName, cellIndex, "", "Number", List(colCss, rowCss)), data2Value))
                }
            }
        }
        cellMap
    }

    def putTableValue(dataFrame: DataFrame, cellMap: Map[(String, String, String), (cell, String => String)]): List[cell] = {
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
//        cellMap.map(x => {
//            val displayName = x._1._1
//            val timeline = x._1._2
//            val colName = x._1._3
//            val unit = colUnitMap.getOrElse(colName,comman)
//            x._2.value = unit(dataFrame.filter(col("DISPLAY_NAME") === displayName)
//                    .filter(col("TIMELINE") === timeline)
//                .select(colName).collectAsList().get(0).toString().replaceAll("[\\[\\]]", ""))
//            x._2
//        }).toList
        cellMap.values.map(x => x._1).toList
    }

    def pushTable(cellList: List[cell], pos: List[Int], slideIndex: Int): Unit = {
        cellList.foreach(x => addCell(x.jobid, x.tableName, x.cell, x.value, x.cate, x.cssName))
        pushCell(cellList.head.jobid, cellList.head.tableName)
        pushExcel(cellList.head.jobid, cellList.head.tableName, pos, slideIndex)
    }
}

class phReportContentTableImpl extends phReportContentTableBaseImpl {

}

class phReportContentTrendsTable extends phReportContentTableBaseImpl {

    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        val data = colValue(colArgs)
        createTable(tableArgs, data)
    }
    def colValue(colArgs: colArgs): Any = {
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
        val result: Any = new growthTable().exec(Map("data" -> colArgs.data, "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName,"dot"), "mktDisplayName" -> colArgs.mktDisplayName))
        result
    }

    def createTable(tableArgs: tableArgs, data: Any): Unit = {
        val cellMap = createTableStyle(tableArgs).map(x => (x._1, x._2._1))
        val cellList = putTableValue(data, cellMap)
        pushTable(cellList, tableArgs.pos, tableArgs.slideIndex)
    }

    def putTableValue(data: Any, cellMap: Map[(String, String, String), cell]): List[cell] = {
        val rdd = data.asInstanceOf[RDD[(String, List[String])]]
        val resultMap = rdd.collect().toMap
        cellMap.foreach(x => {
            x._2.value = resultMap.getOrElse(x._1._1,List.fill(24)("0"))(x._1._2.toInt)
        })
        cellMap.values.toList
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
}

class phReportContentTrendsChart extends phReportContentTrendsTable {
    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
        Thread.sleep(3000)
        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "Line")
        //        Unit
    }
}

class phReportContentComboChart extends phReportContentTrendsTable {
    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
        Thread.sleep(3000)
        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "Combo")
        //        Unit
    }
}

class phReportContentOnlyLineChart extends phReportContentTrendsTable {
    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
        Thread.sleep(3000)
        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "LineNoTable")
        //        Unit
    }
}

class phReportContentBlueGrowthTable extends phReportContentTrendsTable {

    override def createTableStyle(tableArgs: tableArgs): Map[(String, String, String), (cell, String => String)] = {
        var cellMap: Map[(String, String, String), (cell, String => String)] = Map()
        val tableName = UUID.randomUUID().toString
        val rowTitle = tableArgs.rowTitle
        val colTitle = tableArgs.colTitle
        val rowList = tableArgs.rowList
        val colList = tableArgs.colList
        val timelineList = tableArgs.timelineList
        val jobid = tableArgs.jobid
        val col2DataColMap = tableArgs.col2DataColMap
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
            val timeLineCell = getCellCoordinate(timelineIndex + 1, 1)
            addCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
        }
        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowCss = displayNameAndCss._2
            val displayNameTemp = displayNameAndCss._1
            val displayName = displayNameTemp.replaceAll("%", "")
            val rowIndex = displayNameIndex + 2
            val displayCell = "A" + (displayNameIndex + 1).toString + ":" + "A" + (displayNameIndex + 4).toString
            addCell(jobid, tableName, displayCell, displayNameTemp, "String", List(rowTitle._2, rowCss))
            timelineList.zipWithIndex.foreach { case (timelineAndCss, ymIndex) =>
                val timeline = timelineAndCss._1
                val colIndex = ymIndex + 1
                colList.foreach { colNameAndCss =>
                    val colName = col2DataColMap.getOrElse(colNameAndCss._1, colNameAndCss._1)
                    val colCss = colNameAndCss._2
                    val valueCell = getCellCoordinate(colIndex, rowIndex)
                    val data2Value = data2ValueMap.getOrElse(colNameAndCss._1,common)
                    cellMap = cellMap ++ Map((displayName, ymIndex.toString, colName) -> (cell(jobid, tableName, valueCell, "", "Number", List(colCss, rowCss)), data2Value))
                }
            }
        }
        cellMap
    }

    def getCellCoordinate(colIndex: Int, rowIndex: Int): String = {
        if (colIndex <= 12) {
            (colIndex + 65).toChar.toString + rowIndex.toString
        } else {
            (colIndex + 53).toChar.toString + (rowIndex + 2).toString
        }
    }
}

class phReportContentDotAndRmbTable extends phReportContentTableBaseImpl {
    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        colArgs.primaryValueName = "dot"
        colArgs.timelineList = List(colArgs.timelineList.head.replace("DOT ", ""))
        var dataDot = colPrimaryValue(colArgs)
        dataDot = colOtherValue(colArgs, dataDot)
        colArgs.primaryValueName = "LC-RMB"
        var dataRMB = colPrimaryValue(colArgs)
        dataRMB = colOtherValue(colArgs, dataRMB)
        createTable(tableArgs, List(dataDot,dataRMB))
    }


    def createTable(tableArgs: tableArgs, dataLIst: List[DataFrame]): Unit = {
        val cellMap = createTableStyle(tableArgs)
        val cellList = putTableValue(dataLIst, cellMap)
        pushTable(cellList, tableArgs.pos, tableArgs.slideIndex)
    }

    def putTableValue(dataFrameList: List[DataFrame], cellMap: Map[(String, String, String), (cell, String => String)]): List[cell] = {
        dataFrameList.zip(List("DOT ","RMB ")).foreach(data => {
            val dataFrame = data._1
            val common: String => String = x => x
            val dataColNames = dataFrame.columns
            dataFrame.collect().foreach(x => {
                val row = x.toSeq.zip(dataColNames).toList
                val displayName = row.find(x => x._2.equals("DISPLAY_NAME")).get._1.toString
                val timeline = data._2 + row.find(x => x._2.equals("TIMELINE")).get._1.toString
                row.foreach(x => {
                    val oneCell = cellMap.getOrElse((displayName, timeline, x._2),(cell("","","","","",Nil),common))
                    oneCell._1.value = oneCell._2(x._1.toString)
                })
            })
        })
        cellMap.values.map(x => x._1).toList

    }
}

//package com.pharbers.process.stm.step.pptx.slider.content
//
//import java.util.UUID
//import com.pharbers.phsocket.phSocketDriver
//import com.pharbers.process.common.{phCommand, phLyFactory}
//import com.pharbers.spark.phSparkDriver
//import org.apache.poi.xslf.usermodel.XSLFSlide
//import org.apache.spark.sql.DataFrame
//import play.api.libs.json.JsValue
//
//import scala.collection.mutable
//
//object phReportContentTable {
//    val functionMap = Map(
//        "DOT(Mn)" -> "dotMn",
//        "MMU" -> "dot",
//        "Tablet" -> "tablet",
//        "SOM(%)" -> "som",
//        "Growth(%)" -> "GrowthPercentage",
//        "YoY GR(%)" -> "GrowthPercentage",
//        "GR(%)" -> "GrowthPercentage",
//        "RMB" -> "rmb",
//        "RMB(Mn)" -> "rmbMn",
//        "DOT" -> "dot",
//        "SOM in Branded MKT(%)" -> "som",
//        "SOM" -> "som",
//        "Mg(Mn)" -> "dotMn",
//        "MG(Mn)" -> "dotMn",
//        "RMB(Mn)" -> "rmbMn",
//        "" -> "empty"
//    )
//
//    def colName2FunctionName(name: String): String = {
//        functionMap.getOrElse(name, throw new Exception("未定义方法" + name))
//    }
//}
//
//trait phReportContentTable {
//    var slide: XSLFSlide = _
//    val socketDriver = phSocketDriver()
//
//    def pushCell(jobid: String, tableName: String, cell: String, value: String, cate: String, cssName: List[String]): Unit =
//        socketDriver.setExcel(jobid, tableName, cell, value, cate, cssName)
//
//    def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit =
//        socketDriver.excel2PPT(jobid, tableName, pos, sliderIndex)
//
//    //获取timeline开始月份
//    def getStartYm(timeline: String): String = {
//        val ymMap: Map[String, Int] = getTimeLineYm(timeline)
//        val month = ymMap("month")
//        val year = ymMap("year")
//        val ymcount = timelineYmCount(timeline)
//        getymlst(month, year, ymcount - 1)
//    }
//
//    def getymlst(month: Int, year: Int, ymcount: Int): String = {
//        if (ymcount == 0) {
//            if (month < 10) {
//                year.toString + "0" + month.toString
//            } else {
//                year.toString + month.toString
//            }
//        } else {
//            if (month == 1) getymlst(12, year - 1, ymcount - 1)
//            else getymlst(month - 1, year, ymcount - 1)
//        }
//    }
//
//    //计算这张表总共前推多少个月份
//    def dfMonthCount(timelinelst: List[String], collst: List[String]): Int = {
//        val timelineCount = timelinelst.size
//        val colMap: Map[String, Int] = Map("RMB" -> 1, "SOM(%)" -> 1, "Grouth(%)" -> 2)
//        val timelineMax: Int = timelinelst.map(timeline => timelineYmCount(timeline)).max
//        val colMax: Int = collst.map(col => colMap(col)).max
//        val monthCount = timelineMax * colMax * timelineCount
//        monthCount
//    }
//
//    def getTimeLineYm(timeline: String): Map[String, Int] = {
//        val ym = timeline.takeRight(5).split(" ")
//        val month = ym.head.toInt
//        val year = 2000 + ym.last.toInt
//        Map("month" -> month, "year" -> year)
//    }
//
//    //计算timeline需要前推多少个月份
//    def timelineYmCount(timeline: String): Int = {
//        val month = getTimeLineYm(timeline)("month")
//        timeline.split(" ").length match {
//            case 3 => timeline.split(" ")(0) match {
//                case "MAT" => 12
//                case "YTD" => month
//                case "RQ" => 3
//            }
//            case 2 => timeline.charAt(0) match {
//                case 'M' => 1
//                case 'R' => 3
//            }
//        }
//    }
//
//    def tableArgsFormat(args: Map[String, Any]): Map[String, Any] = {
//        val argMap = args.asInstanceOf[Map[String, Any]]
//        //ppt一页
//        slide = argMap("ppt_inc").asInstanceOf[XSLFSlide]
//        //数据
//        val data = argMap("data").asInstanceOf[DataFrame]
//        val element = argMap("element").asInstanceOf[JsValue]
//        val slideIndex = argMap("slideIndex").asInstanceOf[Int]
//        val jobid = argMap("jobid").asInstanceOf[String]
//        //xywh
//        val pos = (element \ "pos").as[List[Int]]
//        //第一行
//        val timelineList = (element \ "timeline").as[List[String]]
//        //第二行
//        val colList = (element \ "col" \ "count").as[List[String]]
//        val titleCol = (element \ "col" \ "title" ).as[String]
//        //第一列
//        val rowList = (element \ "row" \ "display_name").as[List[String]]
//        val mktDisplayName = (element \ "mkt_display").as[String]
//        val mktColName = (element \ "mkt_col").as[String]
//        val titleRow = (element \ "row" \ "title" ).as[String]
//        //表的行数
//        val rowCount = rowList.size + 2
//        //表的列数
//        val colCount = colList.size * timelineList.size + 1
//        //Display Name to DF
//        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
//        import sparkDriver.ss.implicits._
//        val tableDisplayName = (rowList.map(row=>row.replaceAll("%", "")) :+ mktDisplayName)
//            .map(x => x.split(":")(0))
//            .toDF("tableDisplayName")
//            .distinct()
//
//        val tableDF = data.join(tableDisplayName, data("Display Name") === tableDisplayName("tableDisplayName"))
//        //算出的数据
//        var dataMap: mutable.Map[String, Double] = mutable.Map()
//        val tableName = UUID.randomUUID().toString
//        Map("rowList" -> rowList, "jobid" -> jobid, "tableName" -> tableName, "timelineList" -> timelineList,
//            "colList" -> colList, "tableDF" -> tableDF, "dataMap" -> dataMap, "pos" -> pos, "slideIndex" -> slideIndex,
//            "mktDisplayName" -> mktDisplayName, "mktColName" -> mktColName, "colTitle" -> titleCol, "rowTitle" -> titleRow)
//    }
//}
//
//class phReportContentTableImpl extends phReportContentTable with phCommand {
//    override def exec(args: Any): Any = {
//        val argsTmp = args.asInstanceOf[Map[String, Any]]
//        val tableArgsFormatMap: Map[String, Any] = tableArgsFormat(argsTmp)
//        val rowTitleAndCss = tableArgsFormatMap("rowTitle").asInstanceOf[String].split(":")
//        val rowList = tableArgsFormatMap("rowList").asInstanceOf[List[String]]
//        val jobid = tableArgsFormatMap("jobid").asInstanceOf[String]
//        val tableName = tableArgsFormatMap("tableName").asInstanceOf[String]
//        val timelineList = tableArgsFormatMap("timelineList").asInstanceOf[List[String]]
//        val colTitleAndCss = tableArgsFormatMap("colTitle").asInstanceOf[String].split(":")
//        val colList = tableArgsFormatMap("colList").asInstanceOf[List[String]]
//        val tableDF = tableArgsFormatMap("tableDF").asInstanceOf[DataFrame]
//        val dataMap = tableArgsFormatMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
//        val pos = tableArgsFormatMap("pos").asInstanceOf[List[Int]]
//        val slideIndex = tableArgsFormatMap("slideIndex").asInstanceOf[Int]
//        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
//            val rowIndex = displayNameIndex + 3
//            val rowCss = displayNameAndCss.split(":")(1)
//            val displayName = displayNameAndCss.split(":")(0)
//            pushCell(jobid, tableName, "A" + rowIndex.toString, displayName, "String", List(rowCss, rowTitleAndCss(1)))
//            timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
//                val timeline = timelineAndCss.split(":")(0)
//                val timelineCss = timelineAndCss.split(":")(1)
//                colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
//                    val colName = colNameAndCss.split(":")(0)
//                    val colCss = colNameAndCss.split(":")(1)
//                    val startYm: String = getStartYm(timeline)
//                    val ymMap = getTimeLineYm(timeline)
//                    val month = ymMap("month").toString.length match {
//                        case 1 => "0" + ymMap("month")
//                        case _ => ymMap("month")
//                    }
//                    val endYm: String = ymMap("year").toString + month
//                    val colIndex = colList.size * timelineIndex + colNameIndex + 1
//                    val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(colName)
//                    val value = phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
//                        Map("data" -> tableDF, "displayName" -> displayName, "ym" -> timeline, "dataMap" -> dataMap,
//                            "firstRow" -> rowList.head.split(":")(0), "firstCol" -> colList.head.split(":")(0), "startYm" -> startYm,
//                            "lastYm" -> endYm)
//                    )
//                    val cell = (colIndex + 65).toChar.toString + rowIndex.toString
//                    pushCell(jobid, tableName, cell, value.toString, "Number", List(rowCss, colCss))
//                }
//            }
//        }
//        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
//            val timeline = timelineAndCss.split(":")(0)
//            val timelineCss = timelineAndCss.split(":")(1)
//            val cellLeft = (1 + timelineIndex * colList.size + 65).toChar.toString + "1"
//            val cellRight = (timelineIndex * colList.size + colList.size + 65).toChar.toString + "1"
//            val timeLineCell = cellLeft + ":" + cellRight
//            pushCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
//            colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
//                val colName = colNameAndCss.split(":")(0)
//                val colCss = colNameAndCss.split(":")(1)
//                val colCell = (colList.size * timelineIndex + colNameIndex + 1 + 65).toChar.toString + "2"
//                pushCell(jobid, tableName, colCell, colName, "String", List(colTitleAndCss(1), colCss))
//            }
//        }
//        (rowTitleAndCss :: colTitleAndCss :: Nil).zipWithIndex.foreach{
//            case (titleANdCss, index) =>
//                val title = titleANdCss(0)
//                val css = titleANdCss(1)
//                val colCell = "A" + (index + 1)
//                pushCell(jobid, tableName, colCell, title, "String", List(colTitleAndCss(1), css))
//        }
//        pushExcel(jobid, tableName.toString, List(pos.head, pos(1), pos(2), pos(3)), slideIndex)
//    }
//}
//
//class phReportContentTrendsTable extends phReportContentTable with phCommand {
//    override def exec(args: Any): Any = {
//        val argsTmp = args.asInstanceOf[Map[String, Any]]
//        val tableArgsFormatMap: Map[String, Any] = tableArgsFormat(argsTmp)
//        val rowList = tableArgsFormatMap("rowList").asInstanceOf[List[String]]
//        val rowTitleAndCss = tableArgsFormatMap("rowTitle").asInstanceOf[String].split(":")
//        val jobid = tableArgsFormatMap("jobid").asInstanceOf[String]
//        val tableName = tableArgsFormatMap("tableName").asInstanceOf[String]
//        val timelineList = tableArgsFormatMap("timelineList").asInstanceOf[List[String]]
//        val colList = tableArgsFormatMap("colList").asInstanceOf[List[String]]
//        val colTitleAndCss = tableArgsFormatMap("colTitle").asInstanceOf[String].split(":")
//        val tableDF = tableArgsFormatMap("tableDF").asInstanceOf[DataFrame]
//        var dataMap = tableArgsFormatMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
//        val pos = tableArgsFormatMap("pos").asInstanceOf[List[Int]]
//        val slideIndex = tableArgsFormatMap("slideIndex").asInstanceOf[Int]
//        val mktDisplayName = tableArgsFormatMap("mktDisplayName").asInstanceOf[String]
//        val mktColName = tableArgsFormatMap("mktColName").asInstanceOf[String]
//        (rowList :+ mktDisplayName).foreach(displayNameAndCss => {
//            val displayName = displayNameAndCss.split(":")(0)
//            timelineList.foreach(timelineAndCss => {
//                val timeline = timelineAndCss.split(":")(0)
//                val startYm: String = getStartYm(timeline)
//                val ymMap = getTimeLineYm(timeline)
//                val month = ymMap("month").toString.length match {
//                    case 1 => "0" + ymMap("month")
//                    case _ => ymMap("month")
//                }
//                val endYm: String = ymMap("year").toString + month
//                val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(mktColName)
//                phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
//                    Map("data" -> tableDF, "displayName" -> displayName, "ym" -> timeline, "dataMap" -> dataMap,
//                        "firstRow" -> mktDisplayName, "firstCol" -> mktColName, "startYm" -> startYm, "lastYm" -> endYm)
//                )
//            })
//        })
//        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
//            val rowCss = displayNameAndCss.split(":")(1)
//            val displayName = displayNameAndCss.split(":")(0)
//            val rowIndex = displayNameIndex + 2
//            val displayCell = "A" + (displayNameIndex + 2).toString
//            pushCell(jobid, tableName, displayCell, displayName, "String", List(rowCss, rowTitleAndCss(1)))
//            timelineList.zipWithIndex.foreach { case (timelineAndCss, ymIndex) =>
//                val timeline = timelineAndCss.split(":")(0)
//                val startYm: String = getStartYm(timeline)
//                val ymMap = getTimeLineYm(timeline)
//                val month = ymMap("month").toString.length match {
//                    case 1 => "0" + ymMap("month")
//                    case _ => ymMap("month")
//                }
//                val endYm: String = ymMap("year").toString + month
//                val colIndex = ymIndex + 1
//                colList.foreach { colNameAndCss =>
//                    val colName = colNameAndCss.split(":")(0)
//                    val colCss = colNameAndCss.split(":")(1)
//                    val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(colName)
//                    val value = phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
//                        Map("data" -> tableDF, "displayName" -> displayName, "ym" -> timeline, "dataMap" -> dataMap,
//                            "firstRow" -> mktDisplayName, "firstCol" -> mktColName, "startYm" -> startYm,
//                            "lastYm" -> endYm)
//                    )
//                    val valueCell = (colIndex + 65).toChar.toString + rowIndex.toString
//                    pushCell(jobid, tableName, valueCell, value.toString, "Number", List(rowCss, colCss))
//                }
//            }
//        }
//        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
//            val timeline = timelineAndCss.split(":")(0)
//            val timelineCss = timelineAndCss.split(":")(1)
//            val timeLineCell = (timelineIndex + 1 + 65).toChar.toString + "1"
//            pushCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
//        }
//        (rowTitleAndCss  :: Nil).zipWithIndex.foreach{
//            case (titleANdCss, index) =>
//                val title = titleANdCss(0)
//                val css = titleANdCss(1)
//                val colCell = "A" + (index + 1)
//                pushCell(jobid, tableName, colCell, title, "String", List(colTitleAndCss(1), css))
//        }
//        pushExcel(jobid, tableName.toString, List(pos.head, pos(1), pos(2), pos(3)), slideIndex)
//    }
//}
//
//class phReportContentTrendsChart extends phReportContentTrendsTable {
//    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
//        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "Line")
//    }
//}
//
//class phReportContentComboChart extends phReportContentTrendsTable {
//    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
//        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "Combo")
//    }
//}
//
//class phReportContentOnlyLineChart extends phReportContentTrendsTable {
//    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
//        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "LineNoTable")
//    }
//}
//
//class phReportContentBlueGrowthTable extends phReportContentTrendsTable with phCommand {
//    override def exec(args: Any): Any = {
//        val argsTmp = args.asInstanceOf[Map[String, Any]]
//        val tableArgsFormatMap: Map[String, Any] = tableArgsFormat(argsTmp)
//        val rowList = tableArgsFormatMap("rowList").asInstanceOf[List[String]]
//        val rowTitleAndCss = tableArgsFormatMap("rowTitle").asInstanceOf[String].split(":")
//        val jobid = tableArgsFormatMap("jobid").asInstanceOf[String]
//        val tableName = tableArgsFormatMap("tableName").asInstanceOf[String]
//        val timelineList = tableArgsFormatMap("timelineList").asInstanceOf[List[String]]
//        val colList = tableArgsFormatMap("colList").asInstanceOf[List[String]]
//        val colTitleAndCss = tableArgsFormatMap("colTitle").asInstanceOf[String].split(":")
//        val tableDF = tableArgsFormatMap("tableDF").asInstanceOf[DataFrame]
//        var dataMap = tableArgsFormatMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
//        val pos = tableArgsFormatMap("pos").asInstanceOf[List[Int]]
//        val slideIndex = tableArgsFormatMap("slideIndex").asInstanceOf[Int]
//        val mktDisplayName = tableArgsFormatMap("mktDisplayName").asInstanceOf[String]
//        val mktColName = tableArgsFormatMap("mktColName").asInstanceOf[String]
//        (rowList :+ mktDisplayName).foreach(displayNameAndCss => {
//            val displayNameTemp = displayNameAndCss.split(":")(0)
//            val displayName = displayNameTemp.replaceAll("%", "")
//            timelineList.foreach(timelineAndCss => {
//                val timeline = timelineAndCss.split(":")(0)
//                val startYm: String = getStartYm(timeline)
//                val ymMap = getTimeLineYm(timeline)
//                val month = ymMap("month").toString.length match {
//                    case 1 => "0" + ymMap("month")
//                    case _ => ymMap("month")
//                }
//                val endYm: String = ymMap("year").toString + month
//                val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(mktColName)
//                phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
//                    Map("data" -> tableDF, "displayName" -> displayName, "ym" -> timeline, "dataMap" -> dataMap,
//                        "firstRow" -> mktDisplayName, "firstCol" -> mktColName, "startYm" -> startYm, "lastYm" -> endYm)
//                )
//            })
//        })
//        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
//            val rowCss = displayNameAndCss.split(":")(1)
//            val displayNameTemp = displayNameAndCss.split(":")(0)
//            val displayName = displayNameTemp.replaceAll("%","")
//            val rowIndex = displayNameIndex + 2
//            val displayCell = "A" + (displayNameIndex + 1).toString + ":" + "A" + (displayNameIndex + 4).toString
//            pushCell(jobid, tableName, displayCell, displayNameTemp, "String", List(rowCss, rowTitleAndCss(1)))
//            timelineList.zipWithIndex.foreach { case (timelineAndCss, ymIndex) =>
//                val timeline = timelineAndCss.split(":")(0)
//                val startYm: String = getStartYm(timeline)
//                val ymMap = getTimeLineYm(timeline)
//                val month = ymMap("month").toString.length match {
//                    case 1 => "0" + ymMap("month")
//                    case _ => ymMap("month")
//                }
//                val endYm: String = ymMap("year").toString + month
//                val colIndex = ymIndex + 1
//                colList.foreach { colNameAndCss =>
//                    val colName = colNameAndCss.split(":")(0)
//                    val colCss = colNameAndCss.split(":")(1)
//                    val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(colName)
//                    val value = phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
//                        Map("data" -> tableDF, "displayName" -> displayName, "ym" -> timeline, "dataMap" -> dataMap,
//                            "firstRow" -> mktDisplayName, "firstCol" -> mktColName, "startYm" -> startYm,
//                            "lastYm" -> endYm)
//                    )
//                    val valueCell = getCellCoordinate(colIndex, rowIndex)
//                    pushCell(jobid, tableName, valueCell, value.toString, "Number", List(rowCss, colCss))
//                }
//            }
//        }
//        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
//            val timeline = timelineAndCss.split(":")(0)
//            val timelineCss = timelineAndCss.split(":")(1)
//            val timeLineCell = getCellCoordinate(timelineIndex+1, 1)
//            pushCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
//        }
//        pushExcel(jobid, tableName.toString, List(pos.head, pos(1), pos(2), pos(3)), slideIndex)
//    }
//    def getCellCoordinate(colIndex: Int, rowIndex: Int): String ={
//        if (colIndex <= 12){
//            (colIndex + 65).toChar.toString + rowIndex.toString
//        }else {
//            (colIndex + 53).toChar.toString + (rowIndex + 2).toString
//        }
//    }
//}
//
//class phReportContentDoubleSomTable extends  phReportContentTableImpl{
//    override def exec(args: Any): Any = {
//        val argsTmp = args.asInstanceOf[Map[String, Any]]
//        val tableArgsFormatMap: Map[String, Any] = tableArgsFormat(argsTmp)
//        val rowTitleAndCss = tableArgsFormatMap("rowTitle").asInstanceOf[String].split(":")
//        val rowList = tableArgsFormatMap("rowList").asInstanceOf[List[String]]
//        val jobid = tableArgsFormatMap("jobid").asInstanceOf[String]
//        val tableName = tableArgsFormatMap("tableName").asInstanceOf[String]
//        val timelineList = tableArgsFormatMap("timelineList").asInstanceOf[List[String]]
//        val colTitleAndCss = tableArgsFormatMap("colTitle").asInstanceOf[String].split(":")
//        val colList = tableArgsFormatMap("colList").asInstanceOf[List[String]]
//        val tableDF = tableArgsFormatMap("tableDF").asInstanceOf[DataFrame]
//        val dataMap = tableArgsFormatMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
//        val pos = tableArgsFormatMap("pos").asInstanceOf[List[Int]]
//        val slideIndex = tableArgsFormatMap("slideIndex").asInstanceOf[Int]
//        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
//            val rowIndex = displayNameIndex + 3
//            val rowCss = displayNameAndCss.split(":")(1)
//            val displayName = displayNameAndCss.split(":")(0)
//            pushCell(jobid, tableName, "A" + rowIndex.toString, displayName, "String", List(rowCss, rowTitleAndCss(1)))
//            timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
//                val timeline = timelineAndCss.split(":")(0)
//                val timelineCss = timelineAndCss.split(":")(1)
//                colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
//                    val colName = colNameAndCss.split(":").head.split("in").head.replace(" ","")
//                    val mktDisplayName = colNameAndCss.split(":").head.split("in").tail.headOption
//                      .getOrElse(rowList.head.split(":").head)
//                      .replaceFirst(" ", "")
//                    val colCss = colNameAndCss.split(":")(1)
//                    val startYm: String = getStartYm(timeline)
//                    val ymMap = getTimeLineYm(timeline)
//                    val month = ymMap("month").toString.length match {
//                        case 1 => "0" + ymMap("month")
//                        case _ => ymMap("month")
//                    }
//                    val endYm: String = ymMap("year").toString + month
//                    val colIndex = colList.size * timelineIndex + colNameIndex + 1
//                    val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(colName)
//                    val value = phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
//                        Map("data" -> tableDF, "displayName" -> displayName, "ym" -> timeline, "dataMap" -> dataMap,
//                            "firstRow" -> mktDisplayName, "firstCol" -> colList.head.split(":")(0), "startYm" -> startYm,
//                            "lastYm" -> endYm)
//                    )
//                    val cell = (colIndex + 65).toChar.toString + rowIndex.toString
//                    pushCell(jobid, tableName, cell, value.toString, "Number", List(rowCss, colCss))
//                }
//            }
//        }
//        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
//            val timeline = timelineAndCss.split(":")(0)
//            val timelineCss = timelineAndCss.split(":")(1)
//            val cellLeft = (1 + timelineIndex * colList.size + 65).toChar.toString + "1"
//            val cellRight = (timelineIndex * colList.size + colList.size + 65).toChar.toString + "1"
//            val timeLineCell = cellLeft + ":" + cellRight
//            pushCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
//            colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
//                val colName = colNameAndCss.split(":")(0)
//                val colCss = colNameAndCss.split(":")(1)
//                val colCell = (colList.size * timelineIndex + colNameIndex + 1 + 65).toChar.toString + "2"
//                pushCell(jobid, tableName, colCell, colName, "String", List(colTitleAndCss(1), colCss))
//            }
//        }
//        (rowTitleAndCss :: colTitleAndCss :: Nil).zipWithIndex.foreach{
//            case (titleANdCss, index) =>
//                val title = titleANdCss(0)
//                val css = titleANdCss(1)
//                val colCell = "A" + (index + 1)
//                pushCell(jobid, tableName, colCell, title, "String", List(colTitleAndCss(1), css))
//        }
//        pushExcel(jobid, tableName.toString, List(pos.head, pos(1), pos(2), pos(3)), slideIndex)
//    }
//}