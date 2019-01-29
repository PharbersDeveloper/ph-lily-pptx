package com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions

import java.util.UUID

import com.pharbers.process.common.DTO.tableCells
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{argsMapKeys, tableActionBase, tableStageAction}

case class phCreatShowTableAction() extends tableStageAction {
    override val name: String = argsMapKeys.SHOW_TABLE
    override val actionList: List[tableActionBase] = phGetData2CellValueMapAction() :: phGetShowTableTitleStyleAction() :: phGetShowTableHeadStyleAction() ::
            phGetShowTableBodyStyleAction() :: phGetShowTableBodyValueAction() :: Nil

    override def stageReady(args: Map[String, Any]): Map[String, Any] = {
        Map(argsMapKeys.DATA -> args(argsMapKeys.DATA), argsMapKeys.TABLE_SHOW_ARGS -> args(argsMapKeys.TABLE_SHOW_ARGS),
            argsMapKeys.TABLE_CELLS -> tableCells(List(), Map()), argsMapKeys.TABLE_MODEL -> args(argsMapKeys.TABLE_MODEL))
    }

    override def stageClean(args: Map[String, Any], argsNew: Map[String, Any]): Map[String, Any] = {
        args ++ Map(name -> argsNew(argsMapKeys.TABLE_CELLS).asInstanceOf[tableCells].readyCells)
    }
}

class phCreatCityShowTableAction() extends phCreatShowTableAction{
    override val actionList: List[tableActionBase] = phGetData2CellValueMapAction() :: phGetShowTableTitleStyleAction() :: phGetCityShowTableHeadStyleAction() ::
            phGetShowTableBodyStyleAction() :: phGetShowTableBodyValueAction() :: Nil
}

class phCreatCityShowTrendsTableAction() extends phCreatShowTableAction{
    override def stageReady(args: Map[String, Any]): Map[String, Any] = {
        Map(argsMapKeys.DATA -> args(argsMapKeys.DATA), argsMapKeys.TABLE_SHOW_ARGS -> args(argsMapKeys.TABLE_SHOW_ARGS),
            argsMapKeys.TABLE_CELLS -> tableCells(List(), Map()), argsMapKeys.TABLE_MODEL -> args(argsMapKeys.TABLE_MODEL))
    }

    override val actionList: List[tableActionBase] = phGetData2CellValueMapAction() :: phGetCityShowTrendsTableHeadStyleAction() ::
            phGetShowTrendsTableBodyStyleAction() :: phGetShowTrendsTableBodyValueAction() :: Nil
}

class phCreatCityShowMuchTimelineTableAction() extends phCreatShowTableAction{
    override val actionList: List[tableActionBase] = phGetData2CellValueMapAction() :: phGetCityShowMuchTimeLineTableHeadStyleAction() ::
            phGetShowMuchTimelineTableBodyStyleAction() :: phGetShowMuchTimelineTableBodyValueAction() :: Nil
}

class phCreatCityShowStackedTableAction() extends phCreatShowTableAction{
    override def stageReady(args: Map[String, Any]): Map[String, Any] = {
        Map(argsMapKeys.DATA -> args(argsMapKeys.DATA), argsMapKeys.TABLE_SHOW_ARGS -> args(argsMapKeys.TABLE_SHOW_ARGS),
            argsMapKeys.TABLE_CELLS -> tableCells(List(), Map()),
            argsMapKeys.CITY -> args(argsMapKeys.CITY),argsMapKeys.TABLE_MODEL -> args(argsMapKeys.TABLE_MODEL))
    }

    override val actionList: List[tableActionBase] = phGetData2CellValueMapAction() :: phGetCityShowStackedTableHeadStyleAction() ::
            phGetShowStackedTableBodyStyleAction() :: phGetShowCityStackedTableBodyValueAction() :: Nil
}

class phCreatCityShowRankTableAction() extends phCreatShowTableAction{
    override def stageReady(args: Map[String, Any]): Map[String, Any] = {
        Map(argsMapKeys.DATA -> args(argsMapKeys.DATA), argsMapKeys.TABLE_SHOW_ARGS -> args(argsMapKeys.TABLE_SHOW_ARGS),
            argsMapKeys.TABLE_CELLS -> tableCells(List(), Map()), argsMapKeys.TABLE_MODEL -> args(argsMapKeys.TABLE_MODEL))
    }

    override val actionList: List[tableActionBase] = phGetData2CellValueMapAction() :: phGetCityShowTrendsTableHeadStyleAction() ::
            phGetShowTrendsTableBodyStyleAction() :: phGetShowCityRankTableBodyValueAction() :: Nil
}


