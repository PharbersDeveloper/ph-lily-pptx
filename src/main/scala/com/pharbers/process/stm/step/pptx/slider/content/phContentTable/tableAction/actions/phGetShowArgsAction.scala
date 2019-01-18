package com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions

import com.pharbers.process.common.jsonData.{phTable, phTable2Data}
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{argsMapKeys, tableActionBase}
import com.pharbers.process.stm.step.pptx.slider.content.phReportContentTable

case class phGetShowRowListAction() extends tableActionBase{
    override val name: String = argsMapKeys.ROW_LIST

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableModel = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val rowList = tableModel.initOne(tableModel.row.display_name,Map())(phTable2Data.severCss)
        args ++ Map(name -> rowList)
    }
}

case class phGetShowColListAction() extends tableActionBase{
    override val name: String = argsMapKeys.COL_LIST

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableModel = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val colList = tableModel.initOne(tableModel.col.count,phTable2Data.jsonCol2DataColMap)(phTable2Data.severCss)
        args ++ Map(name -> colList)
    }
}

case class phGetShowTimelineListAction() extends tableActionBase{
    override val name: String = argsMapKeys.TIMELINE_LIST

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableModel = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val MonthList = tableModel.initOne(tableModel.timeline,Map())(phTable2Data.removeCssAndSomething("")).asInstanceOf[List[String]]
        val timelineList = tableModel.initOne(MonthList,Map())(phTable2Data.Month2timeLine(phReportContentTable.timelineStar))
        args ++ Map(name -> timelineList)
    }
}

case class phGetShowRowTitleAction() extends tableActionBase{
    override val name: String = argsMapKeys.ROW_TITLE

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableModel = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val rowTitle = phTable2Data.severCss(tableModel.row.title)
        args ++ Map(name -> rowTitle)
    }
}

case class phGetShowColTitleAction() extends tableActionBase{
    override val name: String = argsMapKeys.COL_TITLE

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableModel = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val colTitle = phTable2Data.severCss(tableModel.col.title)
        args ++ Map(name -> colTitle)
    }
}

case class phGetShowCol2DataColMapAction() extends tableActionBase{
    override val name: String = argsMapKeys.COL_2_DATA_COL_MAP

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val col2DataColMap = phTable2Data.jsonShowCol2DataColMap(args(argsMapKeys.MKT_DISPLAY_NAME).asInstanceOf[String])
        args ++ Map(name -> col2DataColMap)
    }
}

case class phGetShowTimelineListFromQuarterAction() extends tableActionBase{
    override val name: String = argsMapKeys.TIMELINE_LIST

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableModel = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val timelineAndCssList = tableModel.initOne(tableModel.timeline,Map())(phTable2Data.quarter2timeLine(phReportContentTable.timelineStar))
                .asInstanceOf[List[String]]
        val timelineList = tableModel.initOne(timelineAndCssList, Map())(phTable2Data.severCss)
        args ++ Map(name -> timelineList)
    }
}