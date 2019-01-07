package com.pharbers.process.stm.step.pptx.slider.content.overview

import com.pharbers.process.stm.step.pptx.slider.content.{colArgs, phReportContentTable, tableArgs}
import org.apache.spark.sql.DataFrame

class phOverViewRankTable extends phReportContentTable{
    override def getColArgs(args: Any): colArgs = ???

    override def getTableArgs(args: Any): tableArgs = ???

    override def colPrimaryValue(colArgs: colArgs): DataFrame = ???

    override def colOtherValue(colArgs: colArgs, data: DataFrame): DataFrame = ???

    override def createTable(tableArgs: tableArgs, data: Any): Unit = ???
}
