package com.pharbers.process.stm.step.pptx.slider.content.overview

import java.util.UUID

import com.pharbers.process.common.phCommand
import com.pharbers.process.stm.step.pptx.slider.content._
import com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol.{ev, movGetValue}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import play.api.libs.json.JsValue

class phOverViewRankTable extends phReportContentTableBaseImpl {
    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        val timelineList = colArgs.timelineList
        colArgs.timelineList = timelineList.head :: Nil
        val rankDf = getRankDF(colArgs)
        val colName = rankDf.columns
        val displayNameList = ((rankDf.limit(20) union rankDf.filter(col("DISPLAY_NAME").isin(colArgs.displayNameList: _*))).collect()
                .map(x => x.toSeq.toList.zip(colName).find(x => x._2.equals("DISPLAY_NAME")).get._1.toString)
                .toList ::: colArgs.displayNameList)
                .distinct
        val rankList = getRank(rankDf, displayNameList, timelineList.head, colArgs.mktDisplayName) ::: timelineList.tail.flatMap(x => {
            colArgs.timelineList = x :: Nil
            val data = getRankDF(colArgs)
            getRank(data, displayNameList, x, colArgs.mktDisplayName)
        })
        colArgs.timelineList = timelineList.head :: Nil
        colArgs.displayNameList = displayNameList
        val data = colOtherValue(colArgs, colPrimaryValue(colArgs))
        tableArgs.rowList = (colArgs.mktDisplayName +: displayNameList).distinct.map(x => (x, tableArgs.rowList.find(m => m._1 == x).getOrElse(("","row_5"))._2))
        createTable(tableArgs, Map("data" -> data, "rank" -> rankList))
    }

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
            val timelineList = (element \ "timeline").as[List[String]].map(x => x.split(":").head).map(x => phReportContentTable.time2timeLine(x))
            val mktDisplayName = ((element \ "mkt_display").as[String] :: rowList.head :: Nil).filter(x => x != "").head
            val displayNameList = rowList.:::((element \ "col" \ "count").as[List[String]].map(x => x.split(":").head.split(" (in|of) ").tail.headOption.getOrElse(""))).::(mktDisplayName)
                    .distinct.filter(x => x != "")
            val primaryValueName = ((element \ "mkt_col").as[String] :: colList.head :: Nil).filter(x => x != "").head
            val data = argsMap("data").asInstanceOf[Map[String, DataFrame]]((element \ "data").as[String])
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
            "MAT Product Growth(%)" -> "GROWTH",
            "YTD Product Growth(%)" -> "GROWTH",
            "Rolling QTR Product Growth(%)" -> "GROWTH",
            "MTH Product Growth(%)" -> "GROWTH",
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
            "RMB(Bn)" -> "LC-RMB",
            "" -> "empty"
        )
        val result: DataFrame = new movGetValue().exec(Map("data" -> colArgs.data, "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList,"mktDisplayName" -> colArgs.mktDisplayName, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName, colArgs.primaryValueName)))
        result
    }

    override def colOtherValue(colArgs: colArgs, data: DataFrame): DataFrame = {
        val sortList = List("growth(%)","som","EV")
        val rowList = colArgs.rowList
        val colList = colArgs.colList.sortBy(x => (sortList.indexOf(x), x))
        val timelineList = colArgs.timelineList
        val mktDisplayName = colArgs.mktDisplayName
        val somCommand: phCommand = new som
        val growthCommand: phCommand = new growth
        val evCommand: phCommand = new ev
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
            "EV" -> evCommand
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
        val cellList = putTableValue(data, cellMap)
        pushTable(cellList, tableArgs.pos, tableArgs.slideIndex)
    }

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
                val colCell = "E" + (index + 1)
                addCell(jobid, tableName, colCell, title, "String", List(css, colTitle._2))
        }


        val timeline = timelineList.head._1
        val timelineCss = timelineList.head._2
        val cellLeft = (5  + 65).toChar.toString + "1"
        val cellRight = (4 + colList.size + 65).toChar.toString + "1"
        val timeLineCell = cellLeft + ":" + cellRight
        addCell(jobid, tableName, timeLineCell, timeline, "String", List(colTitle._2))
        colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
            val colName = colNameAndCss._1
            val colCss = colNameAndCss._2
            val colCell = (colNameIndex + 5 + 65).toChar.toString + "2" + ":" + (colNameIndex + 5 + 65).toChar.toString + "3"
            addCell(jobid, tableName, colCell, colName, "String", List(colCss, colTitle._2))
        }
        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowIndex = displayNameIndex + 4
            val rowCss = displayNameAndCss._2
            val displayName = displayNameAndCss._1
            addCell(jobid, tableName, "E" + rowIndex.toString, displayName, "String", List(rowTitle._2, rowCss))
            val timelineAndCss = timelineList.head
            val timeline = timelineAndCss._1
            val timelineCss = timelineAndCss._2
            colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                val colName = col2DataColMap.getOrElse(colNameAndCss._1, colNameAndCss._1).replace("Share of", "SOM in")
                val data2Value = data2ValueMap.getOrElse(colNameAndCss._1, common)
                val colCss = colNameAndCss._2
                val colIndex = colNameIndex + 1
                val cellIndex = (4 + colIndex + 65).toChar.toString + rowIndex.toString
                cellMap = cellMap ++ Map((displayName, timeline, colName) -> (cell(jobid, tableName, cellIndex, "", "Number", List(colCss, rowCss)), data2Value))
            }

        }
        addCell(jobid, tableName, "A1:D1", colTitle._1, "String", List(colTitle._2))
        addCell(jobid, tableName, "A2:B2", "MAT", "String", List(colTitle._2))
        addCell(jobid, tableName, "C2:D2", "MON", "String", List(colTitle._2))
        addCell(jobid, tableName, "E1:E3", "", "String", List(colTitle._2))
        timelineList.zipWithIndex.foreach{ case (timelineAndCss, timeIndex) =>
            val timeline = timelineAndCss._1
            val timelineCss = timelineAndCss._2
            val timeLineCell = (timeIndex + 65).toChar.toString + "3"
            addCell(jobid, tableName, timeLineCell, timeline.replace("MAT ",""), "String", List(colTitle._2))
            rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
                val rowIndex = displayNameIndex + 4
                val rowCss = displayNameAndCss._2
                val displayName = displayNameAndCss._1
                val cellIndex = (timeIndex + 65).toChar.toString + rowIndex.toString
                cellMap = cellMap ++ Map((displayName, timeline, "") -> (cell(jobid, tableName, cellIndex, "", "String", List("col_common",rowCss)), common))
            }
        }
        cellMap
    }

    override def putTableValue(data: Any, cellMap: Map[(String, String, String), (cell, String => String)]): List[cell] = {
        val common: String => String = x => x
        val dataMap = data.asInstanceOf[Map[String, Any]]
        val dataFrame = dataMap("data").asInstanceOf[DataFrame]
        val rankList = dataMap("rank").asInstanceOf[List[(String,String,String)]]
        rankList.foreach(x => {
            cellMap.getOrElse((x._1,x._2,""),(cell("","","","","",Nil),common))._1.value = x._3
        })
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
        cellMap.values.map(x => x._1).toList
    }

    def getRankDF(colArgs: colArgs): DataFrame = {
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

    def getRank(dataFrame: DataFrame, displayName: Seq[String], timeline: String, mktDisplayName: String): List[(String, String, String)] = {
        val colName = dataFrame.columns
        val rankDisplayNameList = mktDisplayName +: dataFrame.collect().map(x => {
            x.toSeq.toList.zip(colName).find(x => x._2.equals("DISPLAY_NAME")).get._1.toString
        })
        displayName.map(x => {
            (x, timeline, rankDisplayNameList.indexOf(x).toString)
        }).toList
    }
}

class phOverViewRankComboChart extends phOverViewRankTable{
    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        val timelineList = colArgs.timelineList
        colArgs.timelineList = timelineList.head :: Nil
        colArgs.data = colArgs.data
        val rankDf = getRankDF(colArgs)
        val colName = rankDf.columns
        val displayNameList = ((rankDf.limit(20) union rankDf.filter(col("DISPLAY_NAME").isin(colArgs.displayNameList: _*))).collect()
                .map(x => x.toSeq.toList.zip(colName).find(x => x._2.equals("DISPLAY_NAME")).get._1.toString)
                .toList ::: colArgs.displayNameList)
                .distinct
        colArgs.displayNameList = displayNameList
        val data = colOtherValue(colArgs, colPrimaryValue(colArgs))
        tableArgs.rowList = displayNameList.filter(x => x != tableArgs.mktDisplayName)
                .distinct.map(x => (x, tableArgs.rowList.find(m => m._1 == x).getOrElse(("","row_5"))._2))
        createTable(tableArgs, data)
    }

    override def colOtherValue(colArgs: colArgs, data: DataFrame): DataFrame = {
        val colList = colArgs.colList
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
            "MAT Product Growth(%)" -> growthCommand,
            "SOM in Branded MKT(%)" -> somCommand,
            "Share" -> somCommand,
            "som" -> somCommand
        )
        colList.foreach(x => {
            val mktDisplay = x.split(" (in|of) ").tail.headOption.getOrElse(mktDisplayName)
            dataFrame = colMap.getOrElse(x.split(" (in|of) ").head, empty).exec(Map(
                "data" -> dataFrame, "mktDisplayName" -> mktDisplay, "timelineList" -> colArgs.timelineList
            )).asInstanceOf[DataFrame]
        })
        dataFrame
    }

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
//        (rowTitle :: colTitle :: Nil).zipWithIndex.foreach {
//            case (titleANdCss, index) =>
//                val title = titleANdCss._1
//                val css = titleANdCss._2
//                val colCell = "E" + (index + 1)
//                addCell(jobid, tableName, colCell, title, "String", List(css, colTitle._2))
//        }


//        val timeline = timelineList.head._1
//        val timelineCss = timelineList.head._2
//        val cellLeft = (5  + 65).toChar.toString + "1"
//        val cellRight = (4 + colList.size + 65).toChar.toString + "1"
//        val timeLineCell = cellLeft + ":" + cellRight
//        addCell(jobid, tableName, timeLineCell, timeline, "String", List(colTitle._2))
        colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
            val colName = colNameAndCss._1
            val colCss = colNameAndCss._2
            val colCell = (colNameIndex + 1 + 65).toChar.toString + "1"
            addCell(jobid, tableName, colCell, colName, "String", List(colCss, colTitle._2))
        }
        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowIndex = displayNameIndex + 2
            val rowCss = displayNameAndCss._2
            val displayName = displayNameAndCss._1
            addCell(jobid, tableName, "A" + rowIndex.toString, displayName, "String", List(rowTitle._2, rowCss))
            val timelineAndCss = timelineList.head
            val timeline = timelineAndCss._1
            val colName:String => String =  col => col2DataColMap.getOrElse(col, col).replace("Share of", "SOM in")
            val cellIndex: Int => String = colIndex => (colIndex + 65).toChar.toString + rowIndex.toString
            cellMap = cellMap ++ Map((displayName, timeline, colName(colList.head._1)) ->
                    (cell(jobid, tableName, cellIndex(1), "", "Number", List(colList.head._2, rowCss)), common))
            cellMap = cellMap ++ Map(("", displayNameIndex.toString, colName(colList.head._1)) ->
                    (cell(jobid, tableName, cellIndex(2), "", "Number", List(colList.tail.head._1, rowCss)), common))
        }
        cellMap
    }

    override def putTableValue(data: Any, cellMap: Map[(String, String, String), (cell, String => String)]): List[cell] = {
        val common: String => String = x => x
        val dataFrame = data.asInstanceOf[DataFrame]
        val dataColNames = dataFrame.columns
        val dataList = dataFrame.collect()
        val rowAll = dataList.find(x => x.toSeq.head == "all").get.toSeq.zip(dataColNames)
        dataList.zipWithIndex.foreach{ case (x, index) =>
            val row = x.toSeq.zip(dataColNames).toList
            val displayName = row.find(x => x._2.equals("DISPLAY_NAME")).get._1.toString
            val timeline = row.find(x => x._2.equals("TIMELINE")).get._1.toString
            row.foreach(x => {
                val oneCell = cellMap.getOrElse((displayName, timeline, x._2),(cell("","","","","",Nil),common))
                oneCell._1.value = oneCell._2(x._1.toString)
            })
            rowAll.foreach(x => {
                val oneCell = cellMap.getOrElse(("", index.toString, x._2),(cell("","","","","",Nil),common))
                oneCell._1.value = oneCell._2(x._1.toString)
            })
        }

        cellMap.values.map(x => x._1).toList
    }

    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
        Thread.sleep(3000)
        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "Combo")
    }
}

class phOverViewRankGrowthTable extends phOverViewRankTable {
    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        val dataMap = colArgs.data.asInstanceOf[Map[String, DataFrame]]
        val timelineList = colArgs.timelineList
        colArgs.timelineList = timelineList.head :: Nil
        colArgs.data = dataMap("ManufaMNC")
        val rankDf = getRankDF(colArgs)
        val colName = rankDf.columns
        val displayNameList = (colArgs.displayNameList ::: (rankDf.limit(12) union rankDf.filter(col("DISPLAY_NAME").isin(colArgs.displayNameList: _*))).collect()
                .map(x => x.toSeq.toList.zip(colName).find(x => x._2.equals("DISPLAY_NAME")).get._1.toString)
                .toList)
                .distinct
        colArgs.timelineList = timelineList
        colArgs.displayNameList = displayNameList
        colArgs.data = mapping(dataMap("market"), dataMap("movMktOne")) union dataMap("ManufaMNC")
        val data = colOtherValue(colArgs, colPrimaryValue(colArgs))
        tableArgs.rowList = displayNameList.filter(x => x != colArgs.mktDisplayName).distinct.map(x => (x, tableArgs.rowList.find(m => m._1 == x).getOrElse(("","row_5"))._2))
        createTable(tableArgs, data)
    }

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
        val timelineList = (element \ "timeline").as[List[String]].map(x => x.split(":").head).map(x => phReportContentTable.time2timeLine(x))
        val mktDisplayName = ((element \ "mkt_display").as[String] :: rowList.head :: Nil).filter(x => x != "").head
        val displayNameList = rowList.:::((element \ "col" \ "count").as[List[String]].map(x => x.split(":").head.split(" (in|of) ").tail.headOption.getOrElse(""))).::(mktDisplayName)
                .distinct.filter(x => x != "")
        val primaryValueName = ((element \ "mkt_col").as[String] :: colList.head :: Nil).filter(x => x != "").head
        val data = argsMap("data").asInstanceOf[Map[String, DataFrame]]
        colArgs(rowList, colList, timelineList, displayNameList, mktDisplayName, primaryValueName, data)
    }

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

        val title = colTitle._1
        val css = colTitle._2
        val colCell = "A1:A2"
        addCell(jobid, tableName, colCell, title, "String", List(css, colTitle._2))

        addCell(jobid, tableName,"B1:"+  (timelineList.size * colList.size + 65).toChar.toString + "1",
            timelineList.head._1.split(" ").tail.mkString(" "), "String", List(timelineList.head._2, colTitle._2))
        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
            val timeline = timelineAndCss._1
            val timeToHead: String => String = time => {
                Map(
                   "" -> "MTH", "RQ" -> "Rolling QTR", "MAT" -> "MAT", "YTD" -> "YTD"
                ).getOrElse(time.split(" ").head,"MTH")
            }
            colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                val colCss = colNameAndCss._2
                val colCell = (colList.size * timelineIndex + colNameIndex + 1 + 65).toChar.toString + "2"
                addCell(jobid, tableName, colCell, timeToHead(timeline), "String", List(colCss, colTitle._2))
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

    override def putTableValue(data: Any, cellMap: Map[(String, String, String), (cell, String => String)]): List[cell] = {
        val common: String => String = x => x
        val dataFrame = data.asInstanceOf[DataFrame]
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
        cellMap.values.map(x => x._1).toList
    }

    def mapping(dataDF: DataFrame, mapDF: DataFrame): DataFrame = {
        dataDF.join(mapDF, dataDF("ID") === mapDF("ID")).select("DISPLAY_NAME", "DATE", "TYPE", "VALUE").withColumnRenamed("DISPLAY_NAME", "ID")
    }
}
