package com.pharbers.process.stm.step.pptx.slider.content.overview

import java.util.UUID

import com.pharbers.phsocket.phSocketDriver
import com.pharbers.process.common.phCommand
import com.pharbers.process.stm.step.pptx.slider.content.cell
import com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol.mixValue
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

class phOverViewProductPerformanceTable extends phCommand {
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
        val timelineList = colArgs.timelineList
        val data = timelineList.map(x => {
            colArgs.timelineList = x :: Nil
            colValue(colArgs)
        })
        createTable(tableArgs, data)
    }

    def getColArgs(args: Any): colArgs = {
        val col2DataColMap = Map(
            "SOM(%)" -> "som",
            "SOM" -> "som",
            "SOM%" -> "som",
            "Share" -> "som",
            "Growth(%)" -> "Growth(%)",
            "Product Growth(%)" -> "Growth(%)",
            "Mkt Growth(%)" -> "Mkt Growth(%)",
            "GROWTH(%)" -> "Growth(%)",
            "YoY GR(%)" -> "Growth(%)",
            "YoY GR" -> "Growth(%)",
            "YOY GR" -> "Growth(%)",
            "GR(%)" -> "Growth(%)",
            "SOM in Branded MKT(%)" -> "som",
            "SOM in Branded MKT%" -> "som")
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val element = argsMap("element").asInstanceOf[JsValue]
        val rowList = (element \ "row" \ "display_name").as[List[JsValue]].map(x => (x \ "name").as[String].split(":").head.replace("%", ""))
        val rowColList = (element \ "row" \ "display_name").as[List[JsValue]].map(x => (x \ "col").as[String])
        val colList = (element \ "col" \ "count").as[List[String]].map(x => col2DataColMap.getOrElse(x.split(":").head,x.split(":").head))
        val timelineList = (element \ "timeline").as[List[String]].map(x => x.split(":").head)
        val mktDisplayName = ((element \ "mkt_display").as[String] :: rowList.head :: Nil).filter(x => x != "").head
        val displayNameList = rowList.:::((element \ "col" \ "count").as[List[String]].map(x => x.split(":").head.split(" (in|of) ").tail.headOption.getOrElse(""))).::(mktDisplayName)
                .distinct.filter(x => x != "")
        val primaryValueName = ((element \ "mkt_col").as[String] :: colList.head :: Nil).filter(x => x != "").head
        val dataMap = argsMap("data").asInstanceOf[Map[String, DataFrame]]
        val data = dataMap
        val mapping = dataMap
        colArgs(rowList, colList, timelineList, displayNameList,  mktDisplayName,
            primaryValueName, data, mapping, rowColList)
    }

    def getTableArgs(args: Any):tableArgs = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val jobid = argsMap("jobid").asInstanceOf[String]
        val slideIndex = argsMap("slideIndex").asInstanceOf[Int]
        val element = argsMap("element").asInstanceOf[JsValue]
        val rowList = (element \ "row" \ "display_name").as[List[JsValue]].map(x => {
            ((x \ "name").as[String].split(":").head, (x \ "name").as[String].split(":").tail.headOption.getOrElse(""))
        })
        val rowColList = (element \ "row" \ "display_name").as[List[JsValue]].map(x => (x \ "col").as[String])
        val colList = (element \ "col" \ "count").as[List[String]].map(x => (x.split(":").head, x.split(":").tail.headOption.getOrElse("")))
        val timelineList = (element \ "timeline").as[List[String]].map(x => (x.split(":").head, x.split(":").tail.headOption.getOrElse("")))
        val pos = (element \ "pos").as[List[Int]]
        val colTitle = ((element \ "col" \ "title").as[String].split(":").head, (element \ "col" \ "title").as[String].split(":").tail.headOption.getOrElse(""))
        val rowTitle = ((element \ "row" \ "title").as[String].split(":").head, (element \ "row" \ "title").as[String].split(":").tail.headOption.getOrElse(""))
        val mktDisplayName: String = (element \ "mkt_display").as[String]
        val col2DataColMap = Map("DOT(Mn)" -> "RESULT",
            "MMU" -> "RESULT",
            "Tablet" -> "RESULT",
            "SOM(%)" -> "SOM",
            "SOM" -> "SOM",
            "SOM%" -> "SOM",
            "Share" -> "SOM",
            "Growth(%)" -> "GROWTH",
            "GROWTH(%)" -> "GROWTH",
            "YoY GR(%)" -> "GROWTH",
            "YoY GR" -> "GROWTH",
            "YOY GR" -> "GROWTH",
            "GR(%)" -> "GROWTH",
            "RMB" -> "RESULT",
            "RMB(Mn)" -> "RESULT",
            "RMB(Bn)" -> "RESULT",
            "DOT" -> "RESULT",
            "Product Growth(%)" -> "PRODUCT_GROWTH",
            "Mkt Growth(%)" -> "MKT_GROWTH",
            "SOM in Branded MKT(%)" -> "SOM",
            "SOM in Branded MKT%" -> "SOM",
            "Mg(Mn)" -> "RESULT",
            "MG(Mn)" -> "RESULT",
            "RMB(Mn)" -> "RESULT")
        tableArgs(rowList :+ (mktDisplayName, "row_1"), colList, timelineList, mktDisplayName, jobid, pos, colTitle, rowTitle, slideIndex,col2DataColMap, rowColList)
    }

    def colValue(colArgs: colArgs): DataFrame= {
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
        val displayNamesMapping = colArgs.rowList.zip(colArgs.rowColList)
        val dataMap = colArgs.data.asInstanceOf[Map[String, DataFrame]]
        val result: DataFrame = new mixValue().exec(Map("data" -> dataMap("DF_gen_search_set"), "mapData" -> dataMap("movMktFour"), "displayNameList" -> displayNamesMapping, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList,"mktDisplayName" -> colArgs.mktDisplayName, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName, colArgs.primaryValueName)))
                .asInstanceOf[DataFrame]
        result
    }

//    def colOtherValue(colArgs: colArgs, data: DataFrame): DataFrame= {
//        val sortList = List("growth(%)","som","EV")
//        val rowList = colArgs.rowList
//        val colList = colArgs.colList.sortBy(x => (sortList.indexOf(x), x))
//        val timelineList = colArgs.timelineList
//        val mktDisplayName = colArgs.mktDisplayName
//        val somCommand: phCommand = new som
//        val growthCommand: phCommand = new growth
//        val mktGrowthCommand: phCommand = new mktGr
//        val empty: phCommand = new phCommand {
//            override def exec(args: Any): Any = args.asInstanceOf[Map[String, Any]]("data")
//        }
//        var dataFrame = data
//        val colMap = Map(
//            "SOM(%)" -> somCommand,
//            "SOM" -> somCommand,
//            "SOM%" -> somCommand,
//            "Growth(%)" -> growthCommand,
//            "YoY GR(%)" -> growthCommand,
//            "GR(%)" -> growthCommand,
//            "SOM in Branded MKT(%)" -> somCommand,
//            "Share" -> somCommand,
//            "som" -> somCommand,
//            "Mkt Growth(%)" -> mktGrowthCommand
//        )
//        colList.foreach(x => {
//            val mktDisplay = x.split(" (in|of) ").tail.headOption.getOrElse(mktDisplayName)
//            dataFrame = colMap.getOrElse(x.split(" (in|of) ").head, empty).exec(Map(
//                "data" -> dataFrame, "mktDisplayName" -> mktDisplay, "timelineList" -> timelineList
//            )).asInstanceOf[DataFrame]
//        })
//        dataFrame
//    }

    def createTable(tableArgs: tableArgs, data: Any): Unit= {
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
        addCell(jobid, tableName, "B1:B2", colList.head._1, "String", List(colList.head._2, colTitle._2))

        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
            val timeline = timelineAndCss._1
            val timelineCss = timelineAndCss._2
            val cellLeft = (1 + timelineIndex * (colList.size - 1) + 66).toChar.toString + "1"
            val cellRight = ((timelineIndex + 1) * (colList.size - 1) + 66).toChar.toString + "1"
            val timeLineCell = cellLeft + ":" + cellRight
            addCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
            colList.tail.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                val colName = colNameAndCss._1
                val colCss = colNameAndCss._2
                val colCell = ((colList.size - 1) * timelineIndex + colNameIndex + 1 + 66).toChar.toString + "2"
                addCell(jobid, tableName, colCell, colName, "String", List(colCss, colTitle._2))
            }
        }

        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowIndex = displayNameIndex + 3
            val rowCss = displayNameAndCss._2
            val displayName = displayNameAndCss._1
            addCell(jobid, tableName, "A" + rowIndex.toString, displayName, "String", List(rowTitle._2, rowCss))
            cellMap = cellMap ++ Map((displayName, timelineList.head._1, colList.head._1) ->
                    (cell(jobid, tableName, "B" + rowIndex.toString, "", "Number", List(colList.head._2, rowCss)), data2ValueMap.getOrElse(colList.head._1, common)))
            timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
                val timeline = timelineAndCss._1
                val timelineCss = timelineAndCss._2
                colList.tail.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                    val colName = col2DataColMap.getOrElse(colNameAndCss._1, colNameAndCss._1).replace("Share of", "SOM in")
                    val data2Value = data2ValueMap.getOrElse(colNameAndCss._1, common)
                    val colCss = colNameAndCss._2
                    val colIndex = colList.size * timelineIndex + colNameIndex + 1
                    val cellIndex = (colIndex + 66).toChar.toString + rowIndex.toString
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

//    def getDisplayNamesMapping(displayNameList: List[String],displayNameCol: List[String], marketMapDF: DataFrame): List[(String, String, String)] = {
//        ???
//    }

    case class tableArgs(var rowList: List[(String, String)], colList: List[(String, String)],timelineList: List[(String, String)], mktDisplayName: String,
                         jobid: String, pos: List[Int], colTitle: (String, String), rowTitle: (String, String), slideIndex: Int, col2DataColMap: Map[String, String],
                         var rowColList: List[String])

    case class colArgs(var rowList: List[String], var colList: List[String], var timelineList: List[String], var displayNameList: List[String],
                       var mktDisplayName: String, var primaryValueName: String, var data: Any, var mapping: Any = null,var rowColList: List[String])
}

