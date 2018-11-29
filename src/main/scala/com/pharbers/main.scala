package com.pharbers

import java.awt.Rectangle
import java.io.FileOutputStream

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.poi.sl.usermodel.SlideShow
import org.apache.poi.xslf.usermodel.{SlideLayout, XMLSlideShow, XSLFTextRun, XSLFTextShape}

object main extends App {
    phLyFactory.startProcess
//    phLyFactory.setSaveMidDoc
    phLyFactory.getInstance("com.pharbers.process.flow.phBIFlowGenImpl").asInstanceOf[phCommand].exec(null)
    phLyFactory.endProcess
}

object test extends App {
    val ppt: XMLSlideShow = new XMLSlideShow()
    val slide = ppt.createSlide(ppt.getSlideMasters.get(0).getLayout(SlideLayout.TITLE_ONLY))
    val title: XSLFTextShape= slide.getPlaceholder(0)
    val a: XSLFTextRun = title.setText("123")


    title.setAnchor(new Rectangle(0, 0,100,100))


    ppt.write(new FileOutputStream("dcs.pptx"))
}
