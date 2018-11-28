package com.pharbers.process.stm.step.pptx.slider.prop

import java.awt.Rectangle

import com.pharbers.process.common.phCommand
import org.apache.poi.xslf.usermodel.XSLFSlide

trait phReportTitleProp extends phReportProp {

}

class phReportTitlePropImpl extends phReportTitleProp with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        val title = argMap("ppt_inc").asInstanceOf[XSLFSlide].getPlaceholder(0)
        title.setAnchor(new Rectangle(0, 0,10,10))
        title.setText(argMap("title").asInstanceOf[String])
    }
}
