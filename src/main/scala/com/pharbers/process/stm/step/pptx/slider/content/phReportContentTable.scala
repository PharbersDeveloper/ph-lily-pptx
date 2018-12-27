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

trait phReportContentTable {
    var slide: XSLFSlide = _
    val socketDriver = phSocketDriver()

    def pushCell(jobid: String, tableName: String, cell: String, value: String, cate: String, cssName: List[String]): Unit = Unit
//        socketDriver.setExcel(jobid, tableName, cell, value, cate, cssName)


    def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = Unit
//        socketDriver.excel2PPT(jobid, tableName, pos, sliderIndex)

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
        val tableDisplayName = (rowList.map(row=>row.replaceAll("%", "")) :+ mktDisplayName)
            .map(x => x.split(":")(0))
            .toDF("tableDisplayName")
            .distinct()

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
        val tableArgsFormatMap: Map[String, Any] = tableArgsFormat(argsTmp)
        val rowTitleAndCss = tableArgsFormatMap("rowTitle").asInstanceOf[String].split(":")
        val rowList = tableArgsFormatMap("rowList").asInstanceOf[List[String]]
        val jobid = tableArgsFormatMap("jobid").asInstanceOf[String]
        val tableName = tableArgsFormatMap("tableName").asInstanceOf[String]
        val timelineList = tableArgsFormatMap("timelineList").asInstanceOf[List[String]]
        val colTitleAndCss = tableArgsFormatMap("colTitle").asInstanceOf[String].split(":")
        val colList = tableArgsFormatMap("colList").asInstanceOf[List[String]]
        val tableDF = tableArgsFormatMap("tableDF").asInstanceOf[DataFrame]
        val dataMap = tableArgsFormatMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val pos = tableArgsFormatMap("pos").asInstanceOf[List[Int]]
        val slideIndex = tableArgsFormatMap("slideIndex").asInstanceOf[Int]

        /**
         * 这里需要快的话，需要将一个一个计算变成一个表一起计算
         * 1. 其原理为不管在任何情况下，右边下面的display Name都回对应到固定的产品名
         * 那么也就是说整体的display Name是下面display Name的超集
         * 也就是说，可以通过在计算前，计算出一个中间表，在最后通过dispaly Name在做reduce 加法
         * 整体的表只需要做一次计算。
         * 2. SOM 和 Growth 应该和前面的算法一起计算，要不然所有都回多次重复计算
         */

        /**
         * 0. 整理数据源
         */
        val tmp = tableDF.toJavaRDD.rdd.map (x =>
            new phLyCalData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
                BigDecimal(x(5).toString), BigDecimal(x(6).toString), BigDecimal(x(7).toString), x(8).toString))

        tmp.take(10).foreach(println)

        /**
          * 1. 整理所有需要的 display Name
          */
        val lst = "201710" :: "201711" :: "201712" :: "201801" :: "201802" :: "201803" :: "201804" :: "201805" ::
            "201806" :: "201807" :: "201808" :: "201809" :: Nil
        val rows = rowList.map (x => x.split(":")(0))
        val filter_display_name =
            tmp.filter (x => rows.contains(x.display_name)).
                filter(x => lst.contains(x.date)).
                filter(x => x.tp == "LC-RMB")

        filter_display_name.take(10).foreach(println)

        /**
          * 2. reduce by key 就是以display name 求和, 叫中间和
          */
        val mid_sum = filter_display_name.keyBy(x => x.display_name).reduceByKey { (left, right) =>
            left.result =
                (if (left.result == 0) {
                    left.dot
                } else left.result) +
                (if (right.result == 0) {
                    right.dot
                } else right.result)
            left
        }

        mid_sum.take(30).foreach(println)

        //        val tmp =
//        timelineList.map { timeline =>
//            colList.
//        }

//        rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
//            val rowIndex = displayNameIndex + 3
//            val rowCss = displayNameAndCss.split(":")(1)
//            val displayName = displayNameAndCss.split(":")(0)
//            pushCell(jobid, tableName, "A" + rowIndex.toString, displayName, "String", List(rowCss, rowTitleAndCss(1)))
//
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
        val argsTmp = args.asInstanceOf[Map[String, Any]]
        val tableArgsFormatMap: Map[String, Any] = tableArgsFormat(argsTmp)
        val rowList = tableArgsFormatMap("rowList").asInstanceOf[List[String]]
        val rowTitleAndCss = tableArgsFormatMap("rowTitle").asInstanceOf[String].split(":")
        val jobid = tableArgsFormatMap("jobid").asInstanceOf[String]
        val tableName = tableArgsFormatMap("tableName").asInstanceOf[String]
        val timelineList = tableArgsFormatMap("timelineList").asInstanceOf[List[String]]
        val colList = tableArgsFormatMap("colList").asInstanceOf[List[String]]
        val colTitleAndCss = tableArgsFormatMap("colTitle").asInstanceOf[String].split(":")
        val tableDF = tableArgsFormatMap("tableDF").asInstanceOf[DataFrame]
        var dataMap = tableArgsFormatMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val pos = tableArgsFormatMap("pos").asInstanceOf[List[Int]]
        val slideIndex = tableArgsFormatMap("slideIndex").asInstanceOf[Int]
        val mktDisplayName = tableArgsFormatMap("mktDisplayName").asInstanceOf[String]
        val mktColName = tableArgsFormatMap("mktColName").asInstanceOf[String]
        (rowList :+ mktDisplayName).foreach(displayNameAndCss => {
            val displayName = displayNameAndCss.split(":")(0)
            timelineList.foreach(timelineAndCss => {
                val timeline = timelineAndCss.split(":")(0)
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
            pushCell(jobid, tableName, displayCell, displayName, "String", List(rowCss, rowTitleAndCss(1)))
            timelineList.zipWithIndex.foreach { case (timelineAndCss, ymIndex) =>
                val timeline = timelineAndCss.split(":")(0)
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
        (rowTitleAndCss  :: Nil).zipWithIndex.foreach{
            case (titleANdCss, index) =>
                val title = titleANdCss(0)
                val css = titleANdCss(1)
                val colCell = "A" + (index + 1)
                pushCell(jobid, tableName, colCell, title, "String", List(colTitleAndCss(1), css))
        }
        pushExcel(jobid, tableName.toString, List(pos.head, pos(1), pos(2), pos(3)), slideIndex)
    }
}

class phReportContentTrendsChart extends phReportContentTrendsTable {
    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
//        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "Line")
        Unit
    }
}

class phReportContentComboChart extends phReportContentTrendsTable {
    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
//        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "Combo")
        Unit
    }
}

class phReportContentOnlyLineChart extends phReportContentTrendsTable {
    override def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit = {
//        socketDriver.excel2Chart(jobid, tableName, pos, sliderIndex, "LineNoTable")
        Unit
    }
}

class phReportContentBlueGrowthTable extends phReportContentTrendsTable with phCommand {
    override def exec(args: Any): Any = {
        val argsTmp = args.asInstanceOf[Map[String, Any]]
        val tableArgsFormatMap: Map[String, Any] = tableArgsFormat(argsTmp)
        val rowList = tableArgsFormatMap("rowList").asInstanceOf[List[String]]
        val rowTitleAndCss = tableArgsFormatMap("rowTitle").asInstanceOf[String].split(":")
        val jobid = tableArgsFormatMap("jobid").asInstanceOf[String]
        val tableName = tableArgsFormatMap("tableName").asInstanceOf[String]
        val timelineList = tableArgsFormatMap("timelineList").asInstanceOf[List[String]]
        val colList = tableArgsFormatMap("colList").asInstanceOf[List[String]]
        val colTitleAndCss = tableArgsFormatMap("colTitle").asInstanceOf[String].split(":")
        val tableDF = tableArgsFormatMap("tableDF").asInstanceOf[DataFrame]
        var dataMap = tableArgsFormatMap("dataMap").asInstanceOf[mutable.Map[String, Double]]
        val pos = tableArgsFormatMap("pos").asInstanceOf[List[Int]]
        val slideIndex = tableArgsFormatMap("slideIndex").asInstanceOf[Int]
        val mktDisplayName = tableArgsFormatMap("mktDisplayName").asInstanceOf[String]
        val mktColName = tableArgsFormatMap("mktColName").asInstanceOf[String]
        (rowList :+ mktDisplayName).foreach(displayNameAndCss => {
            val displayNameTemp = displayNameAndCss.split(":")(0)
            val displayName = displayNameTemp.replaceAll("%", "")
            timelineList.foreach(timelineAndCss => {
                val timeline = timelineAndCss.split(":")(0)
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
            val displayNameTemp = displayNameAndCss.split(":")(0)
            val displayName = displayNameTemp.replaceAll("%","")
            val rowIndex = displayNameIndex + 2
            val displayCell = "A" + (displayNameIndex + 1).toString + ":" + "A" + (displayNameIndex + 4).toString
            pushCell(jobid, tableName, displayCell, displayNameTemp, "String", List(rowCss, rowTitleAndCss(1)))
            timelineList.zipWithIndex.foreach { case (timelineAndCss, ymIndex) =>
                val timeline = timelineAndCss.split(":")(0)
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
                    val valueCell = getCellCoordinate(colIndex, rowIndex)
                    pushCell(jobid, tableName, valueCell, value.toString, "Number", List(rowCss, colCss))
                }
            }
        }
        timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
            val timeline = timelineAndCss.split(":")(0)
            val timelineCss = timelineAndCss.split(":")(1)
            val timeLineCell = getCellCoordinate(timelineIndex+1, 1)
            pushCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
        }
        pushExcel(jobid, tableName.toString, List(pos.head, pos(1), pos(2), pos(3)), slideIndex)
    }
    def getCellCoordinate(colIndex: Int, rowIndex: Int): String ={
        if (colIndex <= 12){
            (colIndex + 65).toChar.toString + rowIndex.toString
        }else {
            (colIndex + 53).toChar.toString + (rowIndex + 2).toString
        }
    }
}
