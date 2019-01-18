package com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions

import com.pharbers.process.common.DTO.tableShowArgs
import com.pharbers.process.common.jsonData.phTable
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{argsMapKeys, tableActionBase, tableStageAction}

case class phTableShowArgsAction() extends tableStageAction {
    override val name: String = argsMapKeys.TABLE_SHOW_ARGS

    override val actionList: List[tableActionBase] = phGetShowRowListAction() :: phGetShowMktDisplayNameAction() :: phGetShowColListAction() ::
            phGetShowTimelineListAction() :: phGetShowRowTitleAction() :: phGetShowColTitleAction() :: phGetShowCol2DataColMapAction() :: Nil

    override def stageReady(args: Map[String, Any]): Map[String, Any] = {
        Map(argsMapKeys.TABLE_MODEL -> args(argsMapKeys.TABLE_MODEL))
    }

    override def stageClean(args: Map[String, Any], argsNew: Map[String, Any]): Map[String, Any] = {
        val showArgs = tableShowArgs(
            argsNew(argsMapKeys.ROW_LIST).asInstanceOf[List[(String, String)]],
            argsNew(argsMapKeys.COL_LIST).asInstanceOf[List[(String, String)]],
            argsNew(argsMapKeys.TIMELINE_LIST).asInstanceOf[List[(String, String)]],
            argsNew(argsMapKeys.MKT_DISPLAY_NAME).asInstanceOf[String],
            argsNew(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable].pos,
            argsNew(argsMapKeys.COL_TITLE).asInstanceOf[(String, String)],
            argsNew(argsMapKeys.ROW_TITLE).asInstanceOf[(String, String)],
            args(argsMapKeys.SLIDE_INDEX).asInstanceOf[Int],
            argsNew(argsMapKeys.COL_2_DATA_COL_MAP).asInstanceOf[Map[String, String]]
        )

        args ++ Map(name -> showArgs)
    }
}

class phCityTableShowArgsAction() extends phTableShowArgsAction{
    override val actionList: List[tableActionBase] = phGetShowRowListAction() :: phGetShowMktDisplayNameAction() :: phGetShowColListAction() ::
            phGetShowTimelineListFromQuarterAction() :: phGetShowRowTitleAction() :: phGetShowColTitleAction() :: phGetShowCol2DataColMapAction() :: Nil
}