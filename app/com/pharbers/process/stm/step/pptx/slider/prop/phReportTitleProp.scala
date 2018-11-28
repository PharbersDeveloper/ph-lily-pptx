package com.pharbers.process.stm.step.pptx.slider.prop

import com.pharbers.process.common.phCommand
import org.apache.poi.xslf.usermodel.XSLFSlide

trait phReportTitleProp extends phReportProp {

}

class phReportTitlePropImpl extends phReportTitleProp with phCommand {
    override def exec(args: Any): Any = {
        args.asInstanceOf[Map[String, Any]]("ppt_inc").asInstanceOf[XSLFSlide]
    }
}
