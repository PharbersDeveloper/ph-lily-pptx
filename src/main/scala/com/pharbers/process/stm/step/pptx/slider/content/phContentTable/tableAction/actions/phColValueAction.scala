package com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions

import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{argsMapKeys, tableActionBase, tableStageAction}

case class phColValueAction() extends tableStageAction {
    override val name: String = argsMapKeys.DATA
    override val actionList: List[tableActionBase] = phGetColCommandMapAction() :: phColPrimaryValueAction() :: phColOtherValueAction() :: Nil

    override def stageReady(args: Map[String, Any]): Map[String, Any] = {
        Map(argsMapKeys.DATA -> args(argsMapKeys.DATA), argsMapKeys.TABLE_COL_ARGS -> args(argsMapKeys.TABLE_COL_ARGS))
    }

    override def stageClean(args: Map[String, Any], argsNew: Map[String, Any]): Map[String, Any] = {
        args ++ Map(argsMapKeys.DATA -> argsNew(argsMapKeys.DATA))
    }
}

class phColTrendsValueAction extends phColValueAction {
    override val actionList: List[tableActionBase] = phGetColValueAction() :: Nil
}

class phColGPL1TrendsValueAction extends phColValueAction {
    override def stageReady(args: Map[String, Any]): Map[String, Any] = {
        Map(argsMapKeys.DATA -> args(argsMapKeys.DATA), argsMapKeys.TABLE_COL_ARGS -> args(argsMapKeys.TABLE_COL_ARGS),
            argsMapKeys.TABLE_MODEL -> args(argsMapKeys.TABLE_MODEL))
    }
    override val actionList: List[tableActionBase] = phGetGLP1ColValueAction() :: Nil
}

class phColAllValueAction extends phColValueAction {
    override def stageReady(args: Map[String, Any]): Map[String, Any] = {
        Map(argsMapKeys.DATA -> args(argsMapKeys.DATA), argsMapKeys.TABLE_COL_ARGS -> args(argsMapKeys.TABLE_COL_ARGS),
            argsMapKeys.TABLE_MODEL -> args(argsMapKeys.TABLE_MODEL))
    }
    override val actionList: List[tableActionBase] = phGetAllColValueAction() :: Nil
}

class phColStackedValueAction extends phColValueAction {
    override def stageReady(args: Map[String, Any]): Map[String, Any] = {
        Map(argsMapKeys.DATA -> args(argsMapKeys.DATA),
            argsMapKeys.TABLE_COL_ARGS -> args(argsMapKeys.TABLE_COL_ARGS),
            argsMapKeys.CITY -> args(argsMapKeys.CITY),
            argsMapKeys.TABLE_MODEL -> args(argsMapKeys.TABLE_MODEL))
    }
    override val actionList: List[tableActionBase] = phGetColCityStackedCommandMapAction() :: phColCityStackedPrimaryValueAction() :: phColCityStackedOtherValueAction() :: Nil
}

class phColRankValueAction extends phColValueAction {
    override def stageReady(args: Map[String, Any]): Map[String, Any] = {
        Map(argsMapKeys.DATA -> args(argsMapKeys.DATA),
            argsMapKeys.TABLE_COL_ARGS -> args(argsMapKeys.TABLE_COL_ARGS),
            argsMapKeys.TABLE_SHOW_ARGS -> args(argsMapKeys.TABLE_SHOW_ARGS))
    }

    override val actionList: List[tableActionBase] = phGetRankColValueAction() :: phGetRankRowList() :: Nil
}