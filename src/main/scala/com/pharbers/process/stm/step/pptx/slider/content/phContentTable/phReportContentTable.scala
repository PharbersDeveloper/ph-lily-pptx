package com.pharbers.process.stm.step.pptx.slider.content.phContentTable

import com.pharbers.process.common.phCommand
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions._
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{endAction, tableActionBase}

trait phContentTableCommand extends phCommand {
    val actionList: List[tableActionBase]
    override def exec(args: Any): Any = {
        actionList.head.perform(args.asInstanceOf[Map[String, Any]], Some(actionList.tail))
    }
}

class phReportContentTable extends phContentTableCommand{
    val actionList: List[tableActionBase] = phJson2ModelAction() :: phTableColArgsAction() :: phTableShowArgsAction() :: phColValueAction() ::
            phCreatShowTableAction() :: phPushTableAction() :: Nil
}

class phCityBaseTableCommonTable extends phContentTableCommand{
    val actionList: List[tableActionBase] = phJson2ModelAction() :: new phCityTableColArgsAction() :: new phCityTableShowArgsAction() :: phColValueAction() ::
            new phCreatCityShowTableAction() :: phPushTableAction() :: Nil
}

class phCityTrendsTableCommonTable extends phContentTableCommand{
    val actionList: List[tableActionBase] = phJson2ModelAction() :: new phCityTableColArgsAction() ::
            new phCityTableShowArgsAction() :: new phColTrendsValueAction :: new phCreatCityShowTrendsTableAction() :: phPushTableAction() :: Nil

}

class phCityLineChartCommonTable extends phContentTableCommand{
    val actionList: List[tableActionBase] = phJson2ModelAction() :: new phCityTableColArgsAction() ::
            new phCityTableShowArgsAction() :: new phColTrendsValueAction :: new phCreatCityShowTrendsTableAction() :: new phPushLinChartAction() :: Nil

}

class phCityStackedChartCommonTable extends phContentTableCommand{
    val actionList: List[tableActionBase] = phCityStackedJson2ModelAction() :: new phCityTableColArgsAction() ::
            new phCityTableShowArgsAction() :: new phColStackedValueAction() :: new phCreatCityShowStackedTableAction() :: new phPushColumnStackedChartAction() :: Nil

}

class phCityMuchTimelineTableCommonTable extends phContentTableCommand{
    val actionList: List[tableActionBase] = phJson2ModelAction() :: new phCityTableColArgsAction() ::
            new phCityTableShowArgsAction() :: new phColAllValueAction :: new phCreatCityShowMuchTimelineTableAction() :: phPushTableAction() :: Nil

}