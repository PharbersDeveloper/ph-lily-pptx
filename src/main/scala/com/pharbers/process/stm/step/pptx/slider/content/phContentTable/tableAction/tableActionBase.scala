package com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction

object argsMapKeys{
    val TABLE_COL_ARGS = "tableColArgs"
    val TABLE_SHOW_ARGS = "tableShowArgs"
    val TABLE_MODEL = "tableModel"
    val ROW_LIST = "rowList"
    val COL_LIST = "colList"
    val TIMELINE_LIST = "timelineList"
    val MKT_DISPLAY_NAME = "mktDisplayName"
    val DISPLAY_NAME_LIST = "displayNameList"
    val PRIMARY_VALUE_NAME = "primaryValueName"
    val ROW_TITLE = "rowTitle"
    val COL_TITLE = "colTitle"
    val COL_2_DATA_COL_MAP = "col2DataColMap"
    val DATA = "data"
    val JOB_ID = "jobid"
    val POS = "pos"
    val SLIDE_INDEX = "slideIndex"
    val COL_COMMAND_MAP = "colCommandMap"
    val SHOW_TABLE = "showTable"
    val DATA_2_Cell_VALUE_MAP = "data2CellValueMap"
    val TABLE_NAME = "tableName"
    val TABLE_CELLS = "tableCells"
}

trait tableActionBase {
    val name = ""
    def perform(args: Map[String, Any], actionListOp: Option[List[tableActionBase]]): Any ={
        val actionList = actionListOp.getOrElse(List(endAction()))
        actionList.head.perform(show(args), Some(actionList.tail))
    }

    def show(args: Map[String, Any]):  Map[String, Any]
}

abstract class tableStageAction extends tableActionBase{
    val actionList: List[tableActionBase] = Nil

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val showMap = args.asInstanceOf[Map[String, Any]]
        stageClean(showMap, actionList.head.perform(stageReady(showMap), Some(actionList.tail)).asInstanceOf[Map[String, Any]])
    }

    def stageReady(args: Map[String, Any]): Map[String, Any]

    def stageClean(args: Map[String, Any], argsNew: Map[String, Any]): Map[String, Any]
}

case class endAction() extends tableActionBase{
    override val name = "end"

    override def perform(args: Map[String, Any], actionList: Option[List[tableActionBase]]): Any = {
        show(args)
    }

    override def show(args: Map[String, Any]): Map[String, Any] = {
        args
    }
}