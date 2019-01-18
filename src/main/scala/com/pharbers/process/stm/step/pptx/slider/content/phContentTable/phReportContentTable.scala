package com.pharbers.process.stm.step.pptx.slider.content.phContentTable

import com.pharbers.process.common.phCommand
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.tableActionBase

class phReportContentTable extends phCommand{
    val actionList: List[tableActionBase] = Nil

    override def exec(args: Any): Any = {
        actionList.head.perform(args.asInstanceOf[Map[String, Any]], Some(actionList.tail))
    }
}
