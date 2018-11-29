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
        //add配置文件
        title.setAnchor(new Rectangle(100, 10,500,100))
        val run = title.setText(argMap("title").asInstanceOf[String])
        run.setFontSize(24.0)
    }
}
