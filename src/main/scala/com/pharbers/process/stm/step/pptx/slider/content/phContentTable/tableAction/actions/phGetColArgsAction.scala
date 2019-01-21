package com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions

import com.pharbers.process.common.jsonData.{phTable, phTable2Data}
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{argsMapKeys, tableActionBase}
import com.pharbers.process.stm.step.pptx.slider.content.phReportContentTable

case class phGetRowLstAction() extends tableActionBase{
    override val name: String = argsMapKeys.ROW_LIST

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableModel = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val rowList = tableModel.initOne(tableModel.row.display_name,Map())(phTable2Data.removeCssAndSomething("%"))
        args ++ Map(name -> rowList)
    }
}

case class phGetColListAction() extends tableActionBase{
    override val name: String = argsMapKeys.COL_LIST

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableModel = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val colList = tableModel.initOne(tableModel.col.count,phTable2Data.jsonCol2DataColMap)(phTable2Data.removeCssAndSomething(""))
        args ++ Map(name -> colList)
    }
}

case class phGetTimelineListAction() extends tableActionBase{
    override val name: String = argsMapKeys.TIMELINE_LIST

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableModel = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val MonthList = tableModel.initOne(tableModel.timeline,Map())(phTable2Data.removeCssAndSomething("")).asInstanceOf[List[String]]
        val timelineList = tableModel.initOne(MonthList,Map())(phTable2Data.Month2timeLine(phReportContentTable.timelineStar))
        args ++ Map(name -> timelineList)
    }
}

case class phGetMktDisplayNameAction() extends tableActionBase{
    override val name: String = argsMapKeys.MKT_DISPLAY_NAME

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableModel = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val rowList = args(argsMapKeys.ROW_LIST).asInstanceOf[List[String]]
        val mktDisplayName = (tableModel.mkt_display :: rowList.head :: Nil).find(x => x != "").getOrElse("")
        args ++ Map(name  -> mktDisplayName)
    }
}

case class phGetDisplayNameListAction() extends tableActionBase{
    override val name: String = argsMapKeys.DISPLAY_NAME_LIST

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val mktDisplayName = args(argsMapKeys.MKT_DISPLAY_NAME).asInstanceOf[String]
        val rowList = args(argsMapKeys.ROW_LIST).asInstanceOf[List[String]]
        val displayNameList = mktDisplayName +: rowList
        args ++ Map(name  -> displayNameList)
    }
}

case class phGetPrimaryValueNameAction() extends tableActionBase{
    override val name: String = argsMapKeys.PRIMARY_VALUE_NAME

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableModel = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val colList = args(argsMapKeys.COL_LIST).asInstanceOf[List[String]]
        val primaryValueName = phTable2Data.jsonCol2DataColMap.getOrElse((tableModel.mkt_col ::
                colList.head :: Nil).find(x => x != "").getOrElse("dot"), "dot")
        args ++ Map(name -> primaryValueName)
    }
}

case class phGetTimelineListFromQuarterAction() extends tableActionBase{
    override val name: String = argsMapKeys.TIMELINE_LIST

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableModel = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val quarterList = tableModel.initOne(tableModel.timeline,Map())(phTable2Data.removeCssAndSomething("")).asInstanceOf[List[String]]
        val timelineList = tableModel.initOne(quarterList,Map())(phTable2Data.quarter2timeLine(phReportContentTable.timelineStar))
        args ++ Map(name -> timelineList)
    }
}