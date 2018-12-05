package com.pharbers.process.stm.step.pptx.slider.content

import java.awt.{Color, Rectangle}

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.poi.sl.usermodel.TableCell.BorderEdge
import org.apache.poi.xslf.usermodel.{XSLFSlide, XSLFTableCell}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

import scala.collection.mutable

object phReportContentTable{
    val functionMap = Map(
        "DOT(Mn)" -> "dotMn",
        "MMU" -> "dot",
        "Tablet" -> "tablet",
        "SOM(%)" -> "som",
        "Growth(%)" -> "GrowthPercentage",
        "YoY GR(%)" -> "GrowthPercentage",
        "RMB" -> "rmb",
        "SOM in Branded MKT(%)" -> "som"
    )
    def colName2FunctionName(name: String): String = {
        functionMap.getOrElse(name,throw new Exception("未定义方法" + name))
    }
}
trait phReportContentTable {
    var slide: XSLFSlide = _

    def addTable(args: Map[String, Any]): Unit = {

        val argMap = args.asInstanceOf[Map[String, Any]]
        //ppt一页
        slide = argMap("ppt_inc").asInstanceOf[XSLFSlide]
        //数据
        val data = argMap("data").asInstanceOf[DataFrame]
        val element = argMap("element").asInstanceOf[JsValue]
        //xywh
        val pos = (element \ "pos").as[List[Int]]
        //第一行
        val timeline = (element \ "timeline").as[List[String]]
        //第二行

        val colList = (element \ "col").as[List[String]]
        //第一列
        val rowList = (element \ "row").as[List[String]]
        //表的行数
        val rowCount = rowList.size + 2
        //表的列数
        val colCount = colList.size * timeline.size + 1
        //算出的数据
        var dataMap: mutable.Map[String, Double] = mutable.Map()
        //创建table
        //            val table = slide.createTable(rowCount, colCount)
        val table = slide.createTable()
        //设置表格相对于左上角的位置
        val rectangle: Rectangle = new Rectangle(pos.head, pos(1), pos(2), pos(3))
        table.setAnchor(rectangle)
        //TODO：设置表格每一行和列的高度和宽度
        //            table.setColumnWidth(1, 100)
        //            table.getRows.get(0).setHeight(100)
        val timelineRow = table.addRow()
        timelineRow.setHeight(0.8)
        timeline.foreach(x => timelineRow.addCell().setText(x).setFontSize(10.0))
        val firstRow = table.addRow()
        firstRow.setHeight(0.8)
        firstRow.addCell()
        table.setColumnWidth(0, 240)
        timeline.foreach(_ => colList.foreach(x => firstRow.addCell().setText(x).setFontSize(10.0)))
        rowList.foreach(displayName => {
            val row = table.addRow()
            row.setHeight(0.8)
            row.addCell().setText(displayName).setFontSize(10.0)
            timeline.foreach(ym => colList.foreach(colName => {
                val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(colName)
                val value = phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
                    Map("data" -> data, "displayName" -> displayName,
                        "ym" -> ym, "dataMap" -> dataMap, "firstRow" -> rowList.head, "firstCol" -> colList.head)
                )
                row.addCell().setText(value.toString).setFontSize(10.0)
            }))
        })
        (0 until table.getRows.size()).foreach(x => {
            val cells = table.getRows.get(x).getCells
            (0 until cells.size()).foreach(x => setCellBorderColor(cells.get(x), Color.BLACK))
        })
        (1 until  colCount).foreach(x => table.setColumnWidth(x, 65))
    }

    def setCellBorderColor(cell: XSLFTableCell,color: Color): Unit ={
        cell.setBorderColor(BorderEdge.bottom,color)
        cell.setBorderColor(BorderEdge.left,color)
        cell.setBorderColor(BorderEdge.right,color)
        cell.setBorderColor(BorderEdge.top,color)
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
        val element =  argMap("element").asInstanceOf[JsValue]
        val mktDisplayName = (element \ "mkt_display").as[String]
        val mktColName = (element \ "mkt_col").as[String]
        //xywh
        val pos = (element \ "pos").as[List[Int]]
        //第一行
        val timeline = (element \ "timeline").as[List[String]]
        //第二行
        val colList = (element \ "col").as[List[String]]
        //第一列
        val rowList = (element \ "row").as[List[String]]
        //表的行数
        val rowCount = rowList.size + 2
        //表的列数
        val colCount = colList.size * timeline.size + 1
        //算出的数据
        val dataMap: mutable.Map[String, Double] = mutable.Map()
        val table = slide.createTable()
        //设置表格相对于左上角的位置
        val rectangle: Rectangle = new Rectangle(pos.head, pos(1), pos(2), pos(3))
        table.setAnchor(rectangle)
        val timelineRow = table.addRow()
        timelineRow.setHeight(0.8)
        timelineRow.addCell()
        timeline.foreach(x => timelineRow.addCell().setText(x).setFontSize(10.0))
        //            val firstRow = table.addRow()
        //            firstRow.setHeight(0.8)
        //            firstRow.addCell()
        table.setColumnWidth(0, 240)
        //            timeline.foreach(_ => colList.foreach(x => firstRow.addCell().setText(x).setFontSize(10.0)))
        (rowList :+ mktDisplayName).foreach(displayName => {
            timeline.foreach(ym => {
                val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(mktColName)
                phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
                    Map("data" -> data, "displayName" -> displayName,
                        "ym" -> ym, "dataMap" -> dataMap, "firstRow" -> mktDisplayName, "firstCol" -> mktColName)
                )
            })
        })
        rowList.foreach(displayName => {
            val row = table.addRow()
            row.setHeight(0.8)
            row.addCell().setText(displayName).setFontSize(10.0)
            timeline.foreach(ym => colList.foreach(colName => {
                val function = "com.pharbers.process.stm.step.pptx.slider.content." + phReportContentTable.colName2FunctionName(colName)
                val value = phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
                    Map("data" -> data, "displayName" -> displayName,
                        "ym" -> ym, "dataMap" -> dataMap, "firstRow" -> mktDisplayName, "firstCol" -> mktColName)
                )
                row.addCell().setText(value.toString).setFontSize(10.0)
            }))
        })
        (0 until table.getRows.size()).foreach(x => {
            val cells = table.getRows.get(x).getCells
            (0 until cells.size()).foreach(x => setCellBorderColor(cells.get(x), Color.BLACK))
        })
        (1 until  colCount).foreach(x => table.setColumnWidth(x, 65))
    }
}