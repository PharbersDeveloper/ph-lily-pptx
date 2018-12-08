package com.pharbers.process.stm.step.pptx.slider.content

import java.awt.Rectangle
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
        "RMB" -> "rmb",
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

    def pushCell(jobid: String, tableName: String, cell: String, value: String, cate: String): Unit =
        socketDriver.setExcel(jobid, tableName, cell, value, cate)

    def pushExcel(jobid: String, tableName: String, pos: List[Int], sliderIndex: Int): Unit =
        socketDriver.excel2PPT(jobid, tableName, pos, sliderIndex)

    def addTable(args: Map[String, Any]): Unit = {
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
        val colList = (element \ "col").as[List[String]]
        //第一列
        val rowList = (element \ "row").as[List[String]]
        //表的行数
        val rowCount = rowList.size + 2
        //表的列数
        val colCount = colList.size * timelineList.size + 1

        //Display Name to DF
        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        val tableDisplayName = rowList.toDF("tableDisplayName")
        val tableDF = data.join(tableDisplayName, data("Display Name")===tableDisplayName("tableDisplayName"))

        //算出的数据
        var dataMap: mutable.Map[String, Double] = mutable.Map()
        val tableName = UUID.randomUUID().toString
        rowList.zipWithIndex.foreach { case (displayName, displayNameIndex) =>
            val rowIndex = displayNameIndex + 3
            pushCell(jobid, tableName, "A" + rowIndex.toString, displayName, "String")
            timelineList.zipWithIndex.foreach { case (timeline, timelineIndex) =>
                colList.zipWithIndex.foreach { case (colName, colNameIndex) =>
                    val colIndex = colList.size * timelineIndex + colNameIndex + 1
                    val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(colName)
                    val value = phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
                        Map("data" -> tableDF, "displayName" -> displayName,
                            "ym" -> timeline, "dataMap" -> dataMap, "firstRow" -> rowList.head, "firstCol" -> colList.head)
                    )
                    val cell = (colIndex + 65).toChar.toString + rowIndex.toString
                    pushCell(jobid, tableName, cell, value.toString, "Number")
                }
            }
        }
        timelineList.zipWithIndex.foreach { case (timeline, timelineIndex) =>
            val cellLeft = (1 + timelineIndex * colList.size + 65).toChar.toString + "1"
            val cellRight = (timelineIndex * colList.size + colList.size + 65).toChar.toString + "1"
            val timeLineCell = cellLeft + ":" + cellRight
            pushCell(jobid, tableName, timeLineCell, timeline, "String")
            colList.zipWithIndex.foreach { case (colName, colNameIndex) =>
                val colCell = (colList.size * timelineIndex + colNameIndex + 1 + 65).toChar.toString + "2"
                pushCell(jobid, tableName, colCell, colName, "String")
            }
        }
        pushExcel(jobid, tableName.toString, List(pos.head, pos(1), pos(2), pos(3)), slideIndex)
    }
}

class phReportContentTableImpl extends phReportContentTable with phCommand {
    override def exec(args: Any): Any = this.addTable(args.asInstanceOf[Map[String, Any]])
}

class phReportContentTrendsTable extends phReportContentTable with phCommand {
    override def exec(args: Any): Any = this.addTable(args.asInstanceOf[Map[String, Any]])

    override def addTable(args: Map[String, Any]): Unit = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        //ppt一页
        slide = argMap("ppt_inc").asInstanceOf[XSLFSlide]
        //数据
        val data = argMap("data").asInstanceOf[DataFrame]
        //List
        val element = argMap("element").asInstanceOf[JsValue]
        val jobid = argMap("jobid").asInstanceOf[String]
        val slideIndex = argMap("slideIndex").asInstanceOf[Int]
        val mktDisplayName = (element \ "mkt_display").as[String]
        val mktColName = (element \ "mkt_col").as[String]
        //xywh
        val pos = (element \ "pos").as[List[Int]]
        //第一行
        val timelineList = (element \ "timeline").as[List[String]]
        //第二行
        val colList = (element \ "col").as[List[String]]
        //第一列
        val rowList = (element \ "row").as[List[String]]
        //表的行数
        val rowCount = rowList.size + 2
        //表的列数
        val colCount = colList.size * timelineList.size + 1
        //算出的数据
        val dataMap: mutable.Map[String, Double] = mutable.Map()
        val tableName = UUID.randomUUID().toString
        (rowList :+ mktDisplayName).foreach(displayName => {
            timelineList.foreach(ym => {
                val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(mktColName)
                phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
                    Map("data" -> data, "displayName" -> displayName,
                        "ym" -> ym, "dataMap" -> dataMap, "firstRow" -> mktDisplayName, "firstCol" -> mktColName)
                )
            })
        })
        rowList.zipWithIndex.foreach { case (displayName, displayNameIndex) =>
            val rowIndex = displayNameIndex + 2
            val displayCell = "A" + (displayNameIndex+2).toString
            pushCell(jobid,tableName,displayCell,displayName,"String")
            timelineList.zipWithIndex.foreach { case(ym, ymIndex) =>
                val colIndex = ymIndex + 1
                colList.foreach { colName =>
                    val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(colName)
                    val value = phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
                        Map("data" -> data, "displayName" -> displayName,
                            "ym" -> ym, "dataMap" -> dataMap, "firstRow" -> mktDisplayName, "firstCol" -> mktColName)
                    )
                    val valueCell = (colIndex + 65).toChar.toString + rowIndex.toString
                    pushCell(jobid, tableName, valueCell, value.toString, "Number")
                }
            }
        }
        timelineList.zipWithIndex.foreach { case (timeline, timelineIndex) =>
            val timeLineCell = (timelineIndex + 1 + 65).toChar.toString + "1"
            pushCell(jobid, tableName, timeLineCell, timeline, "String")
        }
        pushExcel(jobid, tableName.toString, List(pos.head, pos(1), pos(2), pos(3)), slideIndex)
    }
}
