package com.pharbers.process.stm.step.pptx.slider.content.phContentTable

import com.pharbers.process.common.phCommand
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions._
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{endAction, tableActionBase}

class phReportContentTable extends phCommand{
    val actionList: List[tableActionBase] = phJson2ModelAction() :: phTableColArgsAction() :: phTableShowArgsAction() :: phColValueAction() ::
            phCreatShowTableAction() :: phPushTableAction() :: Nil

    override def exec(args: Any): Any = {
        actionList.head.perform(args.asInstanceOf[Map[String, Any]], Some(actionList.tail))
    }
}

class phCityBaseTableCommonTable extends phCommand{
    val actionList: List[tableActionBase] = phJson2ModelAction() :: new phCityTableColArgsAction() :: new phCityTableShowArgsAction() :: phColValueAction() ::
            phCreatShowTableAction() :: phPushTableAction() :: Nil

    override def exec(args: Any): Any = {
        actionList.head.perform(args.asInstanceOf[Map[String, Any]], Some(actionList.tail))
    }
}
