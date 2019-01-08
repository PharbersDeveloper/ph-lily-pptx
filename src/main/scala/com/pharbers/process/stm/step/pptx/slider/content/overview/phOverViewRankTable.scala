package com.pharbers.process.stm.step.pptx.slider.content.overview

import java.util.UUID

import com.pharbers.process.common.phCommand
import com.pharbers.process.stm.step.pptx.slider.content._
import com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol.{ev, movGetValue}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class phOverViewRankTable extends phReportContentTableBaseImpl {
    override def exec(args: Any): Any = {
        val colArgs = getColArgs(args)
        val tableArgs = getTableArgs(args)
        val timelineList = colArgs.timelineList
        colArgs.timelineList = timelineList.head :: Nil
        colArgs.data = colArgs.data.asInstanceOf[Map[String, DataFrame]]("Manufa")
        val rankDf = getRankDF(colArgs)
        val colName = rankDf.columns
        val displayNameList = ((rankDf.limit(20) union rankDf.filter(col("DISPLAY_NAME").isin(colArgs.displayNameList: _*))).collect()
                .map(x => x.toSeq.toList.zip(colName).find(x => x._2.equals("DISPLAY_NAME")).get._1.toString)
                .toList ::: colArgs.displayNameList)
                .distinct
        val rankList = timelineList.tail.flatMap(x => {
            colArgs.timelineList = x :: Nil
            val data = getRankDF(colArgs)
            getRank(data, displayNameList, x)
        })
        colArgs.timelineList = timelineList.head :: Nil
        colArgs.displayNameList = displayNameList
        val data = colOtherValue(colArgs, colPrimaryValue(colArgs))
        tableArgs.rowList = (colArgs.mktDisplayName +: displayNameList).distinct.map(x => (x, tableArgs.rowList.head._2))
        createTable(tableArgs, Map("data" -> data, "rank" -> rankList))
    }

    //    override def getColArgs(args: Any): colArgs = ???
    //
    //    override def getTableArgs(args: Any): tableArgs = ???

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
        val result: DataFrame = new movGetValue().exec(Map("data" -> colArgs.data, "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colMap.getOrElse(colArgs.primaryValueName, colArgs.primaryValueName)))
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
                val colCell = "E" + (index + 1)
                addCell(jobid, tableName, colCell, title, "String", List(css, colTitle._2))
        }


        val timeline = timelineList.head._1
        val timelineCss = timelineList.head._2
        val cellLeft = (5  + 65).toChar.toString + "1"
        val cellRight = (4 + colList.size + colList.size + 65).toChar.toString + "1"
        val timeLineCell = cellLeft + ":" + cellRight
        addCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
        colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
            val colName = colNameAndCss._1
            val colCss = colNameAndCss._2
            val colCell = (colList.size + colNameIndex + 5 + 65).toChar.toString + "2" + ":" + (colList.size + colNameIndex + 5 + 65).toChar.toString + "3"
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
                val colIndex = colList.size + colNameIndex + 1
                val cellIndex = (4 + colIndex + 65).toChar.toString + rowIndex.toString
                cellMap = cellMap ++ Map((displayName, timeline, colName) -> (cell(jobid, tableName, cellIndex, "", "Number", List(colCss, rowCss)), data2Value))
            }

        }
        addCell(jobid, tableName, "A1:D1", rowTitle._1, "String", List(rowTitle._2))
        addCell(jobid, tableName, "A2:C2", "MAT", "String", List(rowTitle._2))
        addCell(jobid, tableName, "C2:D2", "MON", "String", List(rowTitle._2))
        addCell(jobid, tableName, "E1:E2", "", "String", List(rowTitle._2))
        timelineList.zipWithIndex.foreach{ case (timelineAndCss, timeIndex) =>
            val timeline = timelineAndCss._1
            rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
                val rowIndex = displayNameIndex + 4
                val rowCss = displayNameAndCss._2
                val displayName = displayNameAndCss._1
                val cellIndex = (1 + timeIndex + 65).toChar.toString + rowIndex.toString
                cellMap = cellMap ++ Map((displayName, timeline, "") -> (cell(jobid, tableName, cellIndex, "", "Number", List(rowCss)), common))
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

    def getRank(dataFrame: DataFrame, displayName: Seq[String], timeline: String): List[(String, String, String)] = {
        val colName = dataFrame.columns
        val rankDisplayNameList = dataFrame.collect().map(x => {
            x.toSeq.toList.zip(colName).find(x => x._2.equals("DISPLAY_NAME")).get._1.toString
        })
        displayName.map(x => {
            (x, timeline, rankDisplayNameList.indexOf(x).toString)
        }).toList
    }
}
