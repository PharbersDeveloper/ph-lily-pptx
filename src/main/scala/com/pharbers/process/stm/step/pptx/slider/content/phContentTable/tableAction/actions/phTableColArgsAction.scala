package com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions

import com.pharbers.process.common.DTO.tableColArgs
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{argsMapKeys, tableActionBase, tableStageAction}

case class phTableColArgsAction() extends tableStageAction{
    override val name: String = argsMapKeys.TABLE_COL_ARGS
    override val actionList: List[tableActionBase] = phGetRowLstAction() :: phGetColListAction() :: phGetTimelineListAction() ::
            phGetMktDisplayNameAction() :: phGetDisplayNameListAction() :: phGetPrimaryValueNameAction() :: Nil

    override def stageReady(args: Map[String, Any]): Map[String, Any] = {
        Map(argsMapKeys.TABLE_MODEL -> args(argsMapKeys.TABLE_MODEL))
    }

    override def stageClean(args: Map[String, Any], argsNew: Map[String, Any]): Map[String, Any] = {
        val colArgs: tableColArgs = tableColArgs(argsNew(argsMapKeys.ROW_LIST).asInstanceOf[List[String]],
            argsNew(argsMapKeys.COL_LIST).asInstanceOf[List[String]], argsNew(argsMapKeys.TIMELINE_LIST).asInstanceOf[List[String]],
            argsNew(argsMapKeys.DISPLAY_NAME_LIST).asInstanceOf[List[String]], argsNew(argsMapKeys.MKT_DISPLAY_NAME).asInstanceOf[String],
            argsNew(argsMapKeys.PRIMARY_VALUE_NAME).asInstanceOf[String], args(argsMapKeys.DATA))
        args ++ Map(name -> colArgs)
    }
}

class phCityTableColArgsAction() extends phTableColArgsAction{
    override val actionList: List[tableActionBase] = phGetRowLstAction() :: phGetColListAction() :: phGetTimelineListFromQuarterAction() ::
            phGetMktDisplayNameAction() :: phGetDisplayNameListAction() :: phGetPrimaryValueNameAction() :: Nil
}