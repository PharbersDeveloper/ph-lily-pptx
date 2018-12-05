package com.pharbers

import java.awt.{Color, Rectangle}
import java.io.{FileInputStream, FileOutputStream}
import java.util.Date

import com.pharbers.process.common.{phCommand, phLyFactory}
import com.pharbers.process.stm.step.pptx.slider.content.{phReportTableCol, som}
import org.apache.poi.sl.usermodel.SlideShow
import org.apache.poi.sl.usermodel.TableCell.BorderEdge
import org.apache.poi.xslf.usermodel.{SlideLayout, XMLSlideShow, XSLFTextRun, XSLFTextShape}

object main extends App {
    println(new Date())
    phLyFactory.startProcess
//    phLyFactory.setSaveMidDoc
    phLyFactory.getInstance("com.pharbers.process.flow.phBIFlowGenImpl").asInstanceOf[phCommand].exec(null)
    phLyFactory.endProcess
    println(new Date())
}

object test extends App {
    val ppt: XMLSlideShow = new XMLSlideShow()
    val slide = ppt.createSlide(ppt.getSlideMasters.get(0).getLayout(SlideLayout.TITLE_ONLY))
    val title: XSLFTextShape= slide.getPlaceholder(0)
    title.setAnchor(new Rectangle(100, 10,500,100))
    val a: XSLFTextRun = title.setText("1231111111111111111111111111111111111111111111")
    a.setFontSize(24.0)
    val table = slide.createTable(10, 10)
    table.setAnchor(new Rectangle(-20, 100, 0, 0))
//    table.getRows.get(0).setHeight(100)
    table.setRowHeight(0,100)
    table.setColumnWidth(0, 240)
    table.setColumnWidth(1, 65)
    table.getCell(0,0).setBorderColor(BorderEdge.right, Color.BLACK)
    table.getCell(0,1).setBorderColor(BorderEdge.right, Color.BLACK)

    ppt.write(new FileOutputStream("dcs.pptx"))
}

object test2 extends App with phReportTableCol{
    val ymstr = "RQ10 16"
    val ym = ymstr.substring(1).split(" ")
    val month = ym(0).replaceAll("\\D","").toInt
    val year = 2000 + ym(1).toInt
    val res = getymlst(List(), month, year, 3).map { str =>
        if (str.length == 7) str
        else "0" + str
    }
    println()
}
