package com.pharbers.process.stm.step.pptx.slider.content

import java.util.UUID
import com.pharbers.phsocket.phSocketDriver
import com.pharbers.process.common.{phCommand, phLyFactory}
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
        "RMB(Mn)" -> "rmb",
        "DOT" -> "dot",
        "SOM in Branded MKT(%)" -> "som",
        "Mg(Mn)" -> "dotMn",
        "RMB(Mn)" -> "rmb"
    )

    def colName2FunctionName(name: String): String = {
        functionMap.getOrElse(name, throw new Exception("未定义方法" + name))
    }
}

trait phReportContentTable {
    var slide: XSLFSlide = _
    val socketDriver = phSocketDriver()

    def pushCell(jobid: String, tableName: String, cell: String, value: String, cate: String, cssName: List[String]): Unit =
        socketDriver.setExcel(jobid, tableName, cell, value, cate, cssName)

    def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit =
        socketDriver.excel2PPT(jobid, tableName, pos, sliderIndex)

    //获取timeline开始月份
    def getStartYm(timeline: String): String = {
        val ymMap: Map[String, Int] = getTimeLineYm(timeline)
        val month = ymMap("month")
        val year = ymMap("year")
        val ymcount = timelineYmCount(timeline)
        getymlst(month, year, ymcount - 1)
    }

    def getymlst(month: Int, year: Int, ymcount: Int): String = {
        if (ymcount == 0) {
            if (month < 10) {
                year.toString + "0" + month.toString
            } else {
                year.toString + month.toString
            }
        } else {
            if (month == 1) getymlst(12, year - 1, ymcount - 1)
            else getymlst(month - 1, year, ymcount - 1)
        }
    }

    //计算这张表总共前推多少个月份
    def dfMonthCount(timelinelst: List[String], collst: List[String]): Int = {
        val timelineCount = timelinelst.size
        val colMap: Map[String, Int] = Map("RMB" -> 1, "SOM(%)" -> 1, "Grouth(%)" -> 2)
        val timelineMax: Int = timelinelst.map(timeline => timelineYmCount(timeline)).max
        val colMax: Int = collst.map(col => colMap(col)).max
        val monthCount = timelineMax * colMax * timelineCount
        monthCount
    }

    def getTimeLineYm(timeline: String): Map[String, Int] = {
        val ym = timeline.takeRight(5).split(" ")
        val month = ym.head.toInt
        val year = 2000 + ym.last.toInt
        Map("month" -> month, "year" -> year)
    }

    //计算timeline需要前推多少个月份
    def timelineYmCount(timeline: String): Int = {
        val month = getTimeLineYm(timeline)("month")
        timeline.split(" ").length match {
            case 3 => timeline.split(" ")(0) match {
                case "MAT" => 12
                case "YTD" => month
                case "RQ" => 3
            }
            case 2 => timeline.charAt(0) match {
                case 'M' => 1
                case 'R' => 3
            }

        }
    }

    def tableArgsFormat(args: Map[String, Any]): Map[String, Any] = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        //ppt一页
        slide = argMap("ppt_inc").asInstanceOf[XSLFSlide]
        //数据
        val data = argMap("data").asInstanceOf[DataFrame]
        val element = argMap("element").asInstanceOf[JsValue]
        val slideIndex = argMap("slideIndex").asInstanceOf[Int]
        val jobid = argMap("jobid").asInstanceOf[String]
        //xywh
        val pos = (element \ "pos").as[List[Int]]
        //第一行
        val timelineList = (element \ "timeline").as[List[String]]
        //第二行
        val colList = (element \ "col" \ "count").as[List[String]]
        val titleCol = (element \ "col" \ "title" ).as[String]
        //第一列
        val rowList = (element \ "row" \ "display_name").as[List[String]]
        val mktDisplayName = (element \ "mkt_display").as[String]
        val mktColName = (element \ "mkt_col").as[String]
        val titleRow = (element \ "row" \ "title" ).as[String]
        //表的行数
        val rowCount = rowList.size + 2
        //表的列数
        val colCount = colList.size * timelineList.size + 1
        //Display Name to DF
        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        val tableDisplayName = rowList.toDF("tableDisplayName")
        val tableDF = data.join(tableDisplayName, data("Display Name") === tableDisplayName("tableDisplayName"))
        //算出的数据
        var dataMap: mutable.Map[String, Double] = mutable.Map()
        val tableName = UUID.randomUUID().toString
        Map("rowList" -> rowList, "jobid" -> jobid, "tableName" -> tableName, "timelineList" -> timelineList,
            "colList" -> colList, "tableDF" -> tableDF, "dataMap" -> dataMap, "pos" -> pos, "slideIndex" -> slideIndex,
            "mktDisplayName" -> mktDisplayName, "mktColName" -> mktColName, "colTitle" -> titleCol, "rowTitle" -> titleRow)

    }
}

class phReportContentTableImpl extends phReportContentTable with phCommand {
    override def exec(args: Any): Any = {
        val argsTmp = args.asInstanceOf[Map[String, Any]]
        val rowTitleAndCss = tableArgsFormat(argsTmp)("rowTitle").asInstanceOf[String].split(":")
        val rowList = tableArgsFormat(argsTmp)("rowList").asInstanceOf[List[String]]
        val jobid = tableArgsFormat(argsTmp)("jobid").asInstanceOf[String]
        val tableName = tableArgsFormat(argsTmp)("tableName").asInstanceOf[String]
        val timelineList = tableArgsFormat(argsTmp)("timelineList").asInstanceOf[List[String]]
        val colTitleAndCss = tableArgsFormat(argsTmp)("colTitle").asInstanceOf[String].split(":")
        val colList = tableArgsFormat(argsTmp)("colList").asInstanceOf[List[String]]
        val tableDF = tableArgsFormat(argsTmp)("tableDF").asInstanceOf[DataFrame]
        val dataMap = tableArgsFormat(argsTmp)("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val pos = tableArgsFormat(argsTmp)("pos").asInstanceOf[List[Int]]
        val slideIndex = tableArgsFormat(argsTmp)("slideIndex").asInstanceOf[Int]
        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowIndex = displayNameIndex + 3
            val rowCss = displayNameAndCss.split(":")(1)
            val displayName = displayNameAndCss.split(":")(0)
            pushCell(jobid, tableName, "A" + rowIndex.toString, displayName, "String", List(rowCss, rowTitleAndCss(1)))
            timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
                val timeline = timelineAndCss.split(":")(0)
                val timelineCss = timelineAndCss.split(":")(1)
                colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                    val colName = colNameAndCss.split(":")(0)
                    val colCss = colNameAndCss.split(":")(1)
                    val startYm: String = getStartYm(timeline)
                    val ymMap = getTimeLineYm(timeline)
                    val month = ymMap("month").toString.length match {
                        case 1 => "0" + ymMap("month")
                        case _ => ymMap("month")
                    }
                    val endYm: String = ymMap("year").toString + month
                    val colIndex = colList.size * timelineIndex + colNameIndex + 1
                    val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(colName)
                    val value = phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
                        Map("data" -> tableDF, "displayName" -> displayName, "ym" -> timeline, "dataMap" -> dataMap,
                            "firstRow" -> rowList.head.split(":")(0), "firstCol" -> colList.head.split(":")(0), "startYm" -> startYm,
                            "lastYm" -> endYm)
                    )
                    val cell = (colIndex + 65).toChar.toString + rowIndex.toString
                    pushCell(jobid, tableName, cell, value.toString, "Number", List(rowCss, colCss))
                }
            }
        }
        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
            val timeline = timelineAndCss.split(":")(0)
            val timelineCss = timelineAndCss.split(":")(1)
            val cellLeft = (1 + timelineIndex * colList.size + 65).toChar.toString + "1"
            val cellRight = (timelineIndex * colList.size + colList.size + 65).toChar.toString + "1"
            val timeLineCell = cellLeft + ":" + cellRight
            pushCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
            colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                val colName = colNameAndCss.split(":")(0)
                val colCss = colNameAndCss.split(":")(1)
                val colCell = (colList.size * timelineIndex + colNameIndex + 1 + 65).toChar.toString + "2"
                pushCell(jobid, tableName, colCell, colName, "String", List(colTitleAndCss(1), colCss))
            }
        }
        (rowTitleAndCss :: colTitleAndCss :: Nil).zipWithIndex.foreach{
            case (titleANdCss, index) =>
                val title = titleANdCss(0)
                val css = titleANdCss(1)
                val colCell = "A" + (index + 1)
                pushCell(jobid, tableName, colCell, title, "String", List(colTitleAndCss(1), css))
        }
        pushExcel(jobid, tableName.toString, List(pos.head, pos(1), pos(2), pos(3)), slideIndex)
    }
}

class phReportContentTrendsTable extends phReportContentTable with phCommand {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val rowList = tableArgsFormat(argsMap)("rowList").asInstanceOf[List[String]]
        val jobid = tableArgsFormat(argsMap)("jobid").asInstanceOf[String]
        val tableName = tableArgsFormat(argsMap)("tableName").asInstanceOf[String]
        val timelineList = tableArgsFormat(argsMap)("timelineList").asInstanceOf[List[String]]
        val colList = tableArgsFormat(argsMap)("colList").asInstanceOf[List[String]]
        val tableDF = tableArgsFormat(argsMap)("tableDF").asInstanceOf[DataFrame]
        var dataMap = tableArgsFormat(argsMap)("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val pos = tableArgsFormat(argsMap)("pos").asInstanceOf[List[Int]]
        val slideIndex = tableArgsFormat(argsMap)("slideIndex").asInstanceOf[Int]
        val mktDisplayName = tableArgsFormat(argsMap)("mktDisplayName").asInstanceOf[String]
        val mktColName = tableArgsFormat(argsMap)("mktColName").asInstanceOf[String]
        (rowList :+ mktDisplayName).foreach(displayName => {
            timelineList.foreach(timeline => {
                val startYm: String = getStartYm(timeline)
                val ymMap = getTimeLineYm(timeline)
                val month = ymMap("month").toString.length match {
                    case 1 => "0" + ymMap("month")
                    case _ => ymMap("month")
                }
                val endYm: String = ymMap("year").toString + month
                val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(mktColName)
                phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
                    Map("data" -> tableDF, "displayName" -> displayName, "ym" -> timeline, "dataMap" -> dataMap,
                        "firstRow" -> mktDisplayName, "firstCol" -> mktColName, "startYm" -> startYm, "lastYm" -> endYm)
                )
            })
        })
        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowCss = displayNameAndCss.split(":")(1)
            val displayName = displayNameAndCss.split(":")(0)
            val rowIndex = displayNameIndex + 2
            val displayCell = "A" + (displayNameIndex + 2).toString
            pushCell(jobid, tableName, displayCell, displayName, "String", List(rowCss))
            timelineList.zipWithIndex.foreach { case (timeline, ymIndex) =>
                val startYm: String = getStartYm(timeline)
                val ymMap = getTimeLineYm(timeline)
                val month = ymMap("month").toString.length match {
                    case 1 => "0" + ymMap("month")
                    case _ => ymMap("month")
                }
                val endYm: String = ymMap("year").toString + month
                val colIndex = ymIndex + 1
                colList.foreach { colNameAndCss =>
                    val colName = colNameAndCss.split(":")(0)
                    val colCss = colNameAndCss.split(":")(1)
                    val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(colName)
                    val value = phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
                        Map("data" -> tableDF, "displayName" -> displayName, "ym" -> timeline, "dataMap" -> dataMap,
                            "firstRow" -> mktDisplayName, "firstCol" -> mktColName, "startYm" -> startYm,
                            "lastYm" -> endYm)

                    )
                    val valueCell = (colIndex + 65).toChar.toString + rowIndex.toString
                    pushCell(jobid, tableName, valueCell, value.toString, "Number", List(rowCss, colCss))
                }
            }
        }
        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
            val timeline = timelineAndCss.split(":")(0)
            val timelineCss = timelineAndCss.split(":")(1)
            val timeLineCell = (timelineIndex + 1 + 65).toChar.toString + "1"
            pushCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
        }
        pushExcel(jobid, tableName.toString, List(pos.head, pos(1), pos(2), pos(3)), slideIndex)
    }
}
