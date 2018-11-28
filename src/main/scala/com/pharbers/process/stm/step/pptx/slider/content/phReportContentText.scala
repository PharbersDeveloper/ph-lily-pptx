package com.pharbers.process.stm.step.pptx.slider.content

import java.awt.Rectangle

import com.pharbers.process.common.phCommand
import org.apache.poi.xslf.usermodel.XSLFSlide
import play.api.libs.json.JsValue

trait phReportContentText {

}

class phReportContentTextImpl extends phReportContentText with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        val slide = argMap("ppt_inc").asInstanceOf[XSLFSlide]
        argMap("element").asInstanceOf[JsValue].as[Array[JsValue]].foreach(x => {
            val pos = (x \ "pos").as[Array[Int]]
            val textBox = slide.createTextBox()
            textBox.setAnchor(new Rectangle(pos(0), pos(1), pos(2), pos(3)))
            val r1 = textBox.addNewTextParagraph().addNewTextRun()
            r1.setText((x \ "text").as[String])
            r1.setFontFamily((x \ "font").as[String])
        })
    }
}