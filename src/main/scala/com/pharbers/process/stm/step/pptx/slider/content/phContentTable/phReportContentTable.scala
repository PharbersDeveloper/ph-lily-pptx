package com.pharbers.process.stm.step.pptx.slider.content.phContentTable

import com.pharbers.process.common.phCommand
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions._
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{endAction, tableActionBase}

trait phContentTableCommand extends phCommand {
    val actionList: List[tableActionBase]
    override def exec(args: Any): Any = {
        actionList.head.perform(args.asInstanceOf[Map[String, Any]], actionList.tail)
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
            new phCityTableShowArgsAction() :: new phColTrendsValueAction :: new phCreatCityShowTrendsTableAction() :: new phPushLineChartAction() :: Nil

}

class phCityStackedChartCommonTable extends phContentTableCommand{
    val actionList: List[tableActionBase] = phCityStackedJson2ModelAction() :: new phCityTableColArgsAction() ::
            new phCityTableShowArgsAction() :: new phColStackedValueAction() :: new phCreatCityShowStackedTableAction() :: new phPushColumnStackedChartAction() :: Nil

}

class phCityRankBarStackedChartCommonTable extends phContentTableCommand{
    val actionList: List[tableActionBase] = phCityStackedJson2ModelAction() :: new phCityTableColArgsAction() ::
            new phCityTableShowArgsAction() :: new phColRankStackedValueAction() :: new phCreatCityShowRankStackedTableAction() :: new phPushBarStackedChartAction() :: Nil

}

class phCityMuchTimelineTableCommonTable extends phContentTableCommand{
    val actionList: List[tableActionBase] = phJson2ModelAction() :: new phCityTableColArgsAction() ::
            new phCityTableShowArgsAction() :: new phColAllValueAction :: new phCreatCityShowMuchTimelineTableAction() :: phPushTableAction() :: Nil

}

class phCityGPL1LineChartCommonTable extends phContentTableCommand{
    val actionList: List[tableActionBase] = phJson2ModelAction() :: new phCityTableColArgsAction() ::
            new phCityTableShowArgsAction() :: new phColGPL1TrendsValueAction() :: new phCreatCityShowTrendsTableAction() :: new phPushLineChartAction() :: Nil

}

class phCityRankChartCommonTable extends phContentTableCommand{
    val actionList: List[tableActionBase] = phJson2ModelAction() :: new phCityTableColArgsAction() ::
            new phCityTableShowArgsAction() :: new phColRankValueAction :: new phCreatCityShowRankTableAction() :: new phPushBarChartAction() :: Nil

}

class phCityRankYOYCommonTable extends phContentTableCommand{
    val actionList: List[tableActionBase] = phCityStackedJson2ModelAction() :: new phCityTableColArgsAction() ::
            new phCityTableShowArgsAction() :: new phColRankYOYValueAction :: new phCreatCityShowRankYOYTableAction() :: phPushTableAction() :: Nil

}