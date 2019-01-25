package com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions

import com.pharbers.process.common.DTO.tableColArgs
import com.pharbers.process.common.jsonData.phTable
import com.pharbers.process.common.phCommand
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{argsMapKeys, tableActionBase}
import com.pharbers.process.stm.step.pptx.slider.content._
import com.pharbers.process.stm.step.pptx.slider.content.city.{citySom, phCityColAntiPart, phCityColStacked, phQuarterTableCol}
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
        val colList = colArgs.colList.sortBy(x => (sortList.indexOf(x), x))
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

case class phColCityStackedOtherValueAction() extends tableActionBase {
    override val name: String = argsMapKeys.DATA

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val colArgs = args(argsMapKeys.TABLE_COL_ARGS).asInstanceOf[tableColArgs]
        val cityList = args(argsMapKeys.CITY)
        val sortList = List("som")
        val colList = colArgs.colList.sortBy(x => (sortList.indexOf(x), x))
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
                "data" -> dataFrame, "mktDisplayName" -> mktDisplay, "timelineList" -> timelineList,
                "cityList" -> cityList
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

case class phGetColCityStackedCommandMapAction() extends tableActionBase {
    override val name: String = argsMapKeys.COL_COMMAND_MAP

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val somCommand: phCommand = new citySom
        val growthCommand: phCommand = null
        val colMap = Map(
            "SOM(%)" -> somCommand,
            "SOM" -> somCommand,
            "SOM%" -> somCommand,
            "Growth(%)" -> growthCommand,
            "YoY GR(%)" -> growthCommand,
            "GR(%)" -> growthCommand,
            "SOM in Branded MKT(%)" -> somCommand,
            "Share" -> somCommand,
            "som" -> somCommand
        )

        args ++ Map(name -> colMap)
    }
}

case class phGetColValueAction() extends tableActionBase {
    override val name: String = argsMapKeys.DATA

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val colArgs = args(argsMapKeys.TABLE_COL_ARGS).asInstanceOf[tableColArgs]
        val result = new phQuarterTableCol().getValue(Map("data" -> colArgs.data, "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colArgs.primaryValueName, "mktDisplayName" -> colArgs.mktDisplayName))
        args ++ Map(name -> result)
    }
}

case class phGetGLP1ColValueAction() extends tableActionBase {
    override val name: String = argsMapKeys.DATA

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val colArgs = args(argsMapKeys.TABLE_COL_ARGS).asInstanceOf[tableColArgs]
        val table = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val replaysDisplayMap = table.show_display.flatMap(x => x.col_display_name.map(m => Map(m -> x.show_display_name.split(":").head)))
                .reduce(_ ++ _) ++ Map(colArgs.mktDisplayName -> colArgs.mktDisplayName)
        val result = new ().getValue(Map("data" -> colArgs.data, "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colArgs.primaryValueName,
            "mktDisplayName" -> replaysDisplayMap.keys, "replaysDisplayMap" -> replaysDisplayMap))
        args ++ Map(name -> result)
    }
}

case class phGetAllColValueAction() extends tableActionBase {
    override val name: String = argsMapKeys.DATA

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val colArgs = args(argsMapKeys.TABLE_COL_ARGS).asInstanceOf[tableColArgs]
        val table = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val replaysDisplayMap = table.show_display.flatMap(x => x.col_display_name.map(m => Map(m -> x.show_display_name.split(":").head))).reduce(_ ++ _)
        val result = new phCityColAntiPart().getValue(Map("data" -> colArgs.data, "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colArgs.primaryValueName, "mktDisplayName" -> colArgs.mktDisplayName,
        "replaysDisplayMap" -> replaysDisplayMap))
        args ++ Map(name -> result)
    }
}

case class phColCityStackedPrimaryValueAction() extends tableActionBase{
    override val name: String = argsMapKeys.DATA

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val colArgs = args(argsMapKeys.TABLE_COL_ARGS).asInstanceOf[tableColArgs]
        val table = args(argsMapKeys.TABLE_MODEL).asInstanceOf[phTable]
        val replaysDisplayMap = table.show_display.flatMap(x => x.col_display_name.map(m => Map(m -> x.show_display_name.split(":").head)))
                .reduce(_ ++ _) ++ Map(colArgs.mktDisplayName -> colArgs.mktDisplayName)
        val cityList = args(argsMapKeys.CITY)
        //        val colMap = args(argsMapKeys.COL_COMMAND_MAP).asInstanceOf[Map[String, phCommand]]
        val result = new phCityColStacked().exec(Map("data" -> colArgs.data, "allDisplayNames" -> colArgs.displayNameList, "colList" -> colArgs.colList,
            "timelineList" -> colArgs.timelineList, "primaryValueName" -> colArgs.primaryValueName, "cityList" -> cityList, "replaysDisplayMap" -> replaysDisplayMap))
        args ++ Map(name -> result)
    }
}