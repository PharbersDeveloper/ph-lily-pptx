package com.pharbers.process.stm.step.pptx.slider.content

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, UUID}

import com.pharbers.phsocket.phSocketDriver
import com.pharbers.process.common.{phCommand, phLycalArray, phLycalData}
import com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol.{PieTableCol, PieTableCol2}
import com.pharbers.spark.phSparkDriver
import org.apache.poi.xslf.usermodel.XSLFSlide
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import org.apache.spark.sql.functions.col

import scala.collection.mutable

object phReportContentTable {
    var timelineStar: Date = _
    def initTimeline(time: String): Unit ={
        timelineStar = new SimpleDateFormat("MM yy").parse(time)
    }

    def time2timeLine(time: String): String = {
        val cal = Calendar.getInstance()
        cal.setTime(timelineStar)
        cal.add(Calendar.MONTH, "\\d+".r.findFirstIn("#time\\d+#".r.findFirstIn(time).get).getOrElse("1").toInt - 1)
        time.replaceAll("#time\\d+#", new SimpleDateFormat("MM yy").format(cal.getTime))
    }
}

trait phReportContentTable extends phCommand {
    val socketDriver = phSocketDriver()
    var cells: List[String] = Nil
    def addCell(jobid: String, tableName: String, cell: String, value: String, cate: String, cssName: List[String]): Unit ={
        val css = cssName.mkString("*")
        cells = cells :+ s"#c#$cell#s#$css#t#$cate#v#$value"
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

    def createTable(tableArgs: tableArgs, data: Any): Unit

}

class phReportContentTableBaseImpl extends phReportContentTable {

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
            "Growth Contribution%" -> "GrowthContribution%",
            "SOM in Branded MKT(%)" -> "som",
            "SOM in Branded MKT%" -> "som")
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val element = argsMap("element").asInstanceOf[JsValue]
        val rowList = (element \ "row" \ "display_name").as[List[String]].map(x => x.split(":").head.replace("%", ""))
        val colList = (element \ "col" \ "count").as[List[String]].map(x => col2DataColMap.getOrElse(x.split(":").head,x.split(":").head))
        val timelineList = (element \ "timeline").as[List[String]].map(x => x.split(":").head).map(x => phReportContentTable.time2timeLine(x))
        val mktDisplayName = ((element \ "mkt_display").as[String] :: rowList.head :: Nil).filter(x => x != "").head
        val displayNameList = rowList.:::((element \ "col" \ "count").as[List[String]].map(x => x.split(":").head.split(" (in|of) ").tail.headOption.getOrElse(""))).::(mktDisplayName)
            .distinct.filter(x => x != "")
        val primaryValueName = ((element \ "mkt_col").as[String] :: colList.head :: Nil).filter(x => x != "").head
        val data = argsMap("data")
        colArgs(rowList, colList, timelineList, displayNameList, mktDisplayName, primaryValueName, data)
    }

    override def getTableArgs(args: Any): tableArgs = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val jobid = argsMap("jobid").asInstanceOf[String]
        val slideIndex = argsMap("slideIndex").asInstanceOf[Int]
        val element = argsMap("element").asInstanceOf[JsValue]
        val rowList = (element \ "row" \ "display_name").as[List[String]].map(x => (x.split(":").head, x.split(":").tail.headOption.getOrElse("")))
        val colList = (element \ "col" \ "count").as[List[String]].map(x => (x.split(":").head, x.split(":").tail.headOption.getOrElse("")))
        val timelineList = (element \ "timeline").as[List[String]].map(x => (phReportContentTable.time2timeLine(x.split(":").head), x.split(":").tail.headOption.getOrElse("")))
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
            "GROWTH(%)" -> "GROWTH",
            "YoY GR(%)" -> "GROWTH",
            "YoY GR" -> "GROWTH",
            "YOY GR" -> "GROWTH",
            "GR(%)" -> "GROWTH",
            "Growth Contribution%" -> "GROWTH_CONTRIBUTION",
            "RMB" -> "RESULT",
            "RMB(Mn)" -> "RESULT",
            "RMB(Bn)" -> "RESULT",
            "DOT" -> "RESULT",
            "SOM in Branded MKT(%)" -> ("SOM in " + mktDisplayName),
            "SOM in Branded MKT%" -> ("SOM in " + mktDisplayName),
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
        val growthContribution: phCommand = new growthContribution
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
            "Share" -> somCommand,
            "som" -> somCommand,
            "GrowthContribution%" -> growthContribution
            )
        colList.foreach(x => {
            val mktDisplay = x.split(" (in|of) ").tail.headOption.getOrElse(mktDisplayName)
            dataFrame = colMap.getOrElse(x.split(" (in|of) ").head, empty).exec(Map(
                "data" -> dataFrame, "mktDisplayName" -> mktDisplay, "timelineList" -> timelineList
            )).asInstanceOf[DataFrame]
        })
        dataFrame
    }

    override def createTable(tableArgs: tableArgs, data: Any): Unit = {

        val cellMap = createTableStyle(tableArgs)
        val cellList = putTableValue(data.asInstanceOf[DataFrame], cellMap)
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
        val bn: String => String = x => (x.toDouble / 1000000000).toString
        val data2ValueMap = Map("DOT(Mn)" -> mn,
            "MMU" -> common,
            "Tablet" -> common,
            "RMB" -> common,
            "RMB(Mn)" -> mn,
            "DOT" -> common,
            "Mg(Mn)" -> mn,
            "MG(Mn)" -> mn,
            "RMB(Mn)" -> mn,
            "RMB(Bn)" -> bn)
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

    def putTableValue(data: Any, cellMap: Map[(String, String, String), (cell, String => String)]): List[cell] = {
        val dataFrame = data.asInstanceOf[DataFrame]
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

class phReportContent3DPieChart extends phReportContentTrendsTable {
    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        val data: Any = colValue(colArgs)
        createTable(tableArgs, data)
    }

    override def colValue(colArgs: colArgs): DataFrame = {
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
        val dataMap = colArgs.data.asInstanceOf[Map[String, DataFrame]]
        val result: DataFrame = new PieTableCol().exec(Map("data" -> dataMap("market"), "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName, colArgs.primaryValueName)))
        result
    }

    override def putTableValue(data: Any, cellMap: Map[(String, String, String),  (cell, String => String)]): List[cell] = {
        val dataFrame = data.asInstanceOf[DataFrame]
        val dataColNames = dataFrame.columns
        val common: String => String = x => x
        dataFrame.collect().foreach(x => {
            val row = x.toSeq.zip(dataColNames).toList
            val displayName = row.find(x => x._2.equals("DISPLAY_NAME")).get._1.toString
            row.foreach(x => {
                val oneCell = cellMap.getOrElse((displayName, "0", x._2),(cell("","","","","",Nil),common ))
                oneCell._1.value = x._1.toString
            })
        })
        cellMap.values.map(x => x._1).toList
    }

    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
        Thread.sleep(3000)
        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "Pie3D")
    }
}

class phReportContentPieChart extends phReportContent3DPieChart {

    override def colValue(colArgs: colArgs): DataFrame = {
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
        val dataMap = colArgs.data.asInstanceOf[Map[String, DataFrame]]
        val result: DataFrame = new PieTableCol2().exec(Map("data" -> dataMap("LLYProd"),"mapping" -> dataMap("movMktTwo"), "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName, colArgs.primaryValueName)))
        result
    }


    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
        Thread.sleep(3000)
        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "Pie")
    }
}

class rankTable extends phReportContentTableBaseImpl {
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
        val result: DataFrame = new rank().exec(Map("data" -> colArgs.data, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName, colArgs.primaryValueName)))
        result
    }
}

case class tableArgs(var rowList: List[(String, String)], colList: List[(String, String)], timelineList: List[(String, String)], mktDisplayName: String,
                     jobid: String, pos: List[Int], colTitle: (String, String), rowTitle: (String, String), slideIndex: Int, col2DataColMap: Map[String, String])

case class colArgs(var rowList: List[String], var colList: List[String], var timelineList: List[String], var displayNameList: List[String],
                   var mktDisplayName: String, var primaryValueName: String, var data: Any, var mapping: Any = null)

case class cell(jobid: String, tableName: String, cell: String, var value: String, cate: String, cssName: List[String])