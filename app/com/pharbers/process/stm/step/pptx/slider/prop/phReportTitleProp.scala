package com.pharbers.process.stm.step.pptx.slider.prop

import com.pharbers.process.common.phCommand

trait phReportTitleProp extends phReportProp {

}

class phReportTitlePropImpl extends phReportTitleProp with phCommand {
    override def exec(args: Any): Any = {
        println("title prop")
    }
}
