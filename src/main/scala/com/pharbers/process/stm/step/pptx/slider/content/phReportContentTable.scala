package com.pharbers.process.stm.step.pptx.slider.content

import java.awt.Rectangle

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.poi.xslf.usermodel.XSLFSlide
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

trait phReportContentTable {
    var slide: XSLFSlide = _

    def addTable(args: Map[String, Any]): XSLFSlide = {

        val argMap = args.asInstanceOf[Map[String, Any]]
        //ppt一页
        slide = argMap("ppt_inc").asInstanceOf[XSLFSlide]
        //数据
        val data = argMap("data").asInstanceOf[DataFrame]
        //List
        argMap("element").asInstanceOf[JsValue].as[List[JsValue]].foreach(x => {
            //xywh
            val pos = (x \ "pos").as[List[Int]]
            //第一行
            val timeline = (x \ " timeline").as[List[String]]
            //第二行
            val colList = (x \ " col").as[List[String]]
            //第一列
            val rowList = (x \ " row").as[List[String]]
            //表的行数
            val rowCount = rowList.size + 2
            //表的列数
            val colCount = colList.size * timeline.size + 1
            //创建table
            val table = slide.createTable(rowCount, colCount)
            //设置表格相对于左上角的位置
            val rectangle: Rectangle = new Rectangle(pos.head, pos(2), pos(3), pos(4))
            table.setAnchor(rectangle)
            //TODO：设置表格每一行和列的高度和宽度
            //            table.setColumnWidth(1, 100)
            //            table.getRows.get(0).setHeight(100)
            Array.range(0, rowList.size).map { disPlayNameIndex =>
                val displayName = rowList(disPlayNameIndex)
                val rowIndex = disPlayNameIndex + 2
                Array.range(0, timeline.size).map { timelineIndex =>
                    val ym = timeline(timelineIndex)
                    Array.range(0, colList.size).map { colNameIndex =>
                        val colName = colList(colNameIndex)
                        val colIndex = 3 * timelineIndex + colNameIndex + 1
                        //TODO:需要在这里用col作为key在一个Map中获取对应的计算方法
                        val function = "com.pharbers.process.stm.step.pptx.slider.content" + colName
                        val value = phLyFactory.getInstance(function).asInstanceOf[phCommand].exec(
                            Map("data" -> data, "displayName" -> displayName, "ym" -> ym)
                        )
                        //给单元格赋值
                        table.getRows.get(rowIndex).getCells.get(colIndex).setText(value.toString)
                    }
                }
            }
        })
        slide
    }
}

class phReportContentTableImpl extends phReportContentTable with phCommand {
    override def exec(args: Any): Any = this.addTable(args.asInstanceOf[Map[String, Any]])
}
