package com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions

import com.pharbers.process.common.DTO.tableColArgs
import com.pharbers.process.common.phCommand
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{argsMapKeys, tableActionBase}
import com.pharbers.process.stm.step.pptx.slider.content.{growth, growthContribution, som, valueDF}
import org.apache.spark.sql.DataFrame

case class phColPrimaryValueAction() extends tableActionBase{
    override val name: String = argsMapKeys.DATA

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val colArgs = args(argsMapKeys.TABLE_COL_ARGS).asInstanceOf[tableColArgs]
//        val colMap = args(argsMapKeys.COL_COMMAND_MAP).asInstanceOf[Map[String, phCommand]]
        val result = new valueDF().exec(Map("data" -> colArgs.data, "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colArgs.primaryValueName))
        args ++ Map(name -> result)
    }
}

case class phColOtherValueAction() extends tableActionBase {
    override val name: String = argsMapKeys.DATA

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val colArgs = args(argsMapKeys.TABLE_COL_ARGS).asInstanceOf[tableColArgs]
        val sortList = List("som")
        val colList = colArgs.colList.sortBy(x => (-sortList.indexOf(x), x))
        val timelineList = colArgs.timelineList
        val mktDisplayName = colArgs.mktDisplayName
        val empty: phCommand = new phCommand {
            override def exec(args: Any): Any = args.asInstanceOf[Map[String, Any]]("data")
        }
        var dataFrame = args(argsMapKeys.DATA)
        val colMap = args(argsMapKeys.COL_COMMAND_MAP).asInstanceOf[Map[String, phCommand]]
        colList.foreach(x => {
            val mktDisplay = x.split(" (in|of) ").tail.headOption.getOrElse(mktDisplayName)
            dataFrame = colMap.getOrElse(x.split(" (in|of) ").head, empty).exec(Map(
                "data" -> dataFrame, "mktDisplayName" -> mktDisplay, "timelineList" -> timelineList
            ))
        })
        args ++ Map(argsMapKeys.DATA -> dataFrame)
    }
}

case class phGetColCommandMapAction() extends tableActionBase {
    override val name: String = argsMapKeys.COL_COMMAND_MAP

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val somCommand: phCommand = new som
        val growthCommand: phCommand = new growth
        val growthContribution: phCommand = new growthContribution
        val primaryCommand: phCommand = new valueDF
        val colMap = Map(
            "SOM(%)" -> somCommand,
            "SOM" -> somCommand,
            "SOM%" -> somCommand,
            "Growth(%)" -> growthCommand,
            "YoY GR(%)" -> growthCommand,
            "GR(%)" -> growthCommand,
            "SOM in Branded MKT(%)" -> somCommand,
            "Share" -> somCommand,
            "som" -> somCommand,
            "GrowthContribution%" -> growthContribution
        )

        args ++ Map(name -> colMap)
    }
}

