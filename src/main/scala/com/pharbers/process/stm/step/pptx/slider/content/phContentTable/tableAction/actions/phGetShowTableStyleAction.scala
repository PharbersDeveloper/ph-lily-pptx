package com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions

import com.pharbers.process.common.DTO.{cell, tableCells, tableShowArgs}
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{argsMapKeys, tableActionBase}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

case class phGetData2CellValueMapAction() extends tableActionBase{
    override val name: String = argsMapKeys.DATA_2_Cell_VALUE_MAP

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val common: String => String = x => x
        val mn: String => String = x => (x.toDouble / 1000000).toString
        val bn: String => String = x => (x.toDouble / 1000000000).toString
        val data2ValueMap = Map("DOT(Mn)" -> mn,
            "MMU" -> common,
            "Tablet" -> common,
            "RMB" -> common,
            "RMB(Mn)" -> mn,
            "DOT" -> common,
            "Mg(Mn)" -> mn,
            "MG(Mn)" -> mn,
            "RMB(Mn)" -> mn,
            "RMB(Bn)" -> bn
        )
        args ++ Map(name -> data2ValueMap)
    }
}

case class phGetShowTableTitleStyleAction() extends tableActionBase{
    override val name: String = "get show table title style"

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableShowArgs = args(argsMapKeys.TABLE_SHOW_ARGS).asInstanceOf[tableShowArgs]
        val tableCells = args(argsMapKeys.TABLE_CELLS).asInstanceOf[tableCells]
        (tableShowArgs.rowTitle :: tableShowArgs.colTitle :: Nil).zipWithIndex.foreach {
            case (titleANdCss, index) =>
                val title = titleANdCss._1
                val css = titleANdCss._2
                val css2 = tableShowArgs.colTitle._2
                val colCell = "A" + (index + 1)
                tableCells.readyCells = tableCells.readyCells :+  s"#c#$colCell#v#$title#t#String#s#$css*$css2"
        }
        args
    }
}

case class phGetShowTableHeadStyleAction() extends tableActionBase{
    override val name: String = "get show table head style"

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableShowArgs = args(argsMapKeys.TABLE_SHOW_ARGS).asInstanceOf[tableShowArgs]
        val tableCells = args(argsMapKeys.TABLE_CELLS).asInstanceOf[tableCells]

        tableShowArgs.timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
            val timeline = timelineAndCss._1
            val timelineCss = timelineAndCss._2
            val cellLeft = (1 + timelineIndex * tableShowArgs.colList.size + 65).toChar.toString + "1"
            val cellRight = (timelineIndex * tableShowArgs.colList.size + tableShowArgs.colList.size + 65).toChar.toString + "1"
            val timeLineCell = cellLeft + ":" + cellRight
//            addCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
            tableCells.readyCells = tableCells.readyCells :+  s"#c#$timeLineCell#v#$timeline#t#String#s#$timelineCss"
            tableShowArgs.colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                val colName = colNameAndCss._1
                val colCss = colNameAndCss._2
                val colTitleCss = tableShowArgs.colTitle._2
                val colCell = (tableShowArgs.colList.size * timelineIndex + colNameIndex + 1 + 65).toChar.toString + "2"
//                addCell(jobid, tableName, colCell, colName, "String", List(colCss, colTitleCss))
                tableCells.readyCells = tableCells.readyCells :+  s"#c#$colCell#v#$colName#t#String#s#$colCss*$colTitleCss"
            }
        }
        args
    }
}

case class phGetShowTableBodyStyleAction() extends tableActionBase{
    override val name: String = "get show table body style"

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableShowArgs = args(argsMapKeys.TABLE_SHOW_ARGS).asInstanceOf[tableShowArgs]
        val tableCells = args(argsMapKeys.TABLE_CELLS).asInstanceOf[tableCells]

        tableShowArgs.rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowIndex = displayNameIndex + 3
            val rowCss = displayNameAndCss._2
            val displayName = displayNameAndCss._1
            val displayNemeCellIndex = "A" + rowIndex.toString
            val rowTitleCss = tableShowArgs.rowTitle._2
//            addCell(jobid, tableName, "A" + rowIndex.toString, displayName, "String", List(rowTitle._2, rowCss))
            tableCells.readyCells = tableCells.readyCells :+  s"#c#$displayNemeCellIndex#v#$displayName#t#String#s#$rowTitleCss*$rowCss"

            tableShowArgs.timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
                val timeline = timelineAndCss._1
                val timelineCss = timelineAndCss._2
                tableShowArgs.colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                    val colName = tableShowArgs.col2DataColMap.getOrElse(colNameAndCss._1, colNameAndCss._1).replace("Share of", "SOM in")
                    val data2ValueMap = args(argsMapKeys.DATA_2_Cell_VALUE_MAP).asInstanceOf[Map[String, String => String]]
                    val data2Value = data2ValueMap.getOrElse(colNameAndCss._1, data2ValueMap("DOT"))
                    val colCss = colNameAndCss._2
                    val colIndex = tableShowArgs.colList.size * timelineIndex + colNameIndex + 1
                    val cellIndex = (colIndex + 65).toChar.toString + rowIndex.toString
//                    cellMap = cellMap ++ Map((displayName, timeline, colName) -> (cell(jobid, tableName, cellIndex, "", "Number", List(colCss, rowCss)), data2Value))
                    tableCells.noValueCells = tableCells.noValueCells ++
                            Map((displayName, timeline, colName) -> cell(cellIndex, "", "Number", List(colCss, rowCss), data2Value))
                }
            }
        }
        args
    }
}

case class phGetShowTableBodyValueAction() extends tableActionBase {
    override val name: String = "put data value into table body"

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableCells = args(argsMapKeys.TABLE_CELLS).asInstanceOf[tableCells]
        val dataFrame = args(argsMapKeys.DATA).asInstanceOf[DataFrame]
        val dataColNames = dataFrame.columns

        dataFrame.collect().foreach(x => {
            val row = x.toSeq.zip(dataColNames).toList
            val displayName = row.find(x => x._2.equals("DISPLAY_NAME")).get._1.toString
            val timeline = row.find(x => x._2.equals("TIMELINE")).get._1.toString
            row.foreach(x => {
                val oneCell = tableCells.noValueCells.getOrElse((displayName, timeline, x._2),cell("","","",Nil))
                oneCell.setValue(x._1.toString)
            })
        })
        tableCells.allReady()
        args
    }
}

case class phGetCityShowTableHeadStyleAction() extends tableActionBase{
    override val name: String = "get show table head style"

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableShowArgs = args(argsMapKeys.TABLE_SHOW_ARGS).asInstanceOf[tableShowArgs]
        val tableCells = args(argsMapKeys.TABLE_CELLS).asInstanceOf[tableCells]

        tableShowArgs.timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
            val timeline = timelineAndCss._1
            val month = "\\d\\d".r.findFirstIn("Q\\d\\d".r.findFirstIn(timeline).get).get.toInt
            val qtimeline = timeline.replace(month.toString,(month / 3).toString)
            val timelineCss = timelineAndCss._2
            val cellLeft = (1 + timelineIndex * tableShowArgs.colList.size + 65).toChar.toString + "1"
            val cellRight = (timelineIndex * tableShowArgs.colList.size + tableShowArgs.colList.size + 65).toChar.toString + "1"
            val timeLineCell = cellLeft + ":" + cellRight
            //            addCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
            tableCells.readyCells = tableCells.readyCells :+  s"#c#$timeLineCell#v#$qtimeline#t#String#s#$timelineCss"
            tableShowArgs.colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                val colName = colNameAndCss._1
                val colCss = colNameAndCss._2
                val colTitleCss = tableShowArgs.colTitle._2
                val colCell = (tableShowArgs.colList.size * timelineIndex + colNameIndex + 1 + 65).toChar.toString + "2"
                //                addCell(jobid, tableName, colCell, colName, "String", List(colCss, colTitleCss))
                tableCells.readyCells = tableCells.readyCells :+  s"#c#$colCell#v#$colName#t#String#s#$colCss*$colTitleCss"
            }
        }
        args
    }
}

case class phGetCityShowTrendsTableHeadStyleAction() extends tableActionBase{
    override val name: String = "get show table head style"

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableShowArgs = args(argsMapKeys.TABLE_SHOW_ARGS).asInstanceOf[tableShowArgs]
        val tableCells = args(argsMapKeys.TABLE_CELLS).asInstanceOf[tableCells]

        tableShowArgs.timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
            val timeline = timelineAndCss._1
            val month = "\\d\\d".r.findFirstIn("Q\\d\\d".r.findFirstIn(timeline).get).get.toInt
            val qtimeline = timeline.replace(month.toString,(month / 3).toString)
            val timelineCss = timelineAndCss._2
            val cellLeft = (1 + timelineIndex * tableShowArgs.colList.size + 65).toChar.toString + "1"
            val cellRight = (timelineIndex * tableShowArgs.colList.size + tableShowArgs.colList.size + 65).toChar.toString + "1"
            val timeLineCell = cellLeft + ":" + cellRight
            //            addCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
            tableCells.readyCells = tableCells.readyCells :+  s"#c#$timeLineCell#v#$qtimeline#t#String#s#$timelineCss"
        }
        args
    }
}

case class phGetCityShowStackedTableHeadStyleAction() extends tableActionBase{
    override val name: String = "get show table head style"

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableShowArgs = args(argsMapKeys.TABLE_SHOW_ARGS).asInstanceOf[tableShowArgs]
        val tableCells = args(argsMapKeys.TABLE_CELLS).asInstanceOf[tableCells]
        val cityList = args(argsMapKeys.CITY).asInstanceOf[List[String]]
        cityList.zipWithIndex.foreach { case (city, index) =>
            val cellLeft = (1 + index * tableShowArgs.colList.size + 65).toChar.toString + "1"
            val cellRight = (index * tableShowArgs.colList.size + tableShowArgs.colList.size + 65).toChar.toString + "1"
            val cellIndex = cellLeft + ":" + cellRight
            //            addCell(jobid, tableName, timeLineCell, timeline, "String", List(timelineCss))
            tableCells.readyCells = tableCells.readyCells :+  s"#c#$cellIndex#v#$city#t#String#s#timeline_2"
        }
        args
    }
}

case class phGetShowTrendsTableBodyStyleAction() extends tableActionBase{
    override val name: String = "get show table body style"

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableShowArgs = args(argsMapKeys.TABLE_SHOW_ARGS).asInstanceOf[tableShowArgs]
        val tableCells = args(argsMapKeys.TABLE_CELLS).asInstanceOf[tableCells]

        tableShowArgs.rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowIndex = displayNameIndex + 2
            val rowCss = displayNameAndCss._2
            val displayName = displayNameAndCss._1
            val displayNemeCellIndex = "A" + rowIndex.toString
            val rowTitleCss = tableShowArgs.rowTitle._2
            //            addCell(jobid, tableName, "A" + rowIndex.toString, displayName, "String", List(rowTitle._2, rowCss))
            tableCells.readyCells = tableCells.readyCells :+  s"#c#$displayNemeCellIndex#v#$displayName#t#String#s#$rowTitleCss*$rowCss"

            tableShowArgs.timelineList.zipWithIndex.foreach { case (timelineAndCss, timelineIndex) =>
//                val timeline = timelineAndCss._1
//                val timelineCss = timelineAndCss._2
                tableShowArgs.colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                    val colName = tableShowArgs.col2DataColMap.getOrElse(colNameAndCss._1, colNameAndCss._1).replace("Share of", "SOM in")
                    val data2ValueMap = args(argsMapKeys.DATA_2_Cell_VALUE_MAP).asInstanceOf[Map[String, String => String]]
                    val data2Value = data2ValueMap.getOrElse(colNameAndCss._1, data2ValueMap("DOT"))
                    val colCss = colNameAndCss._2
                    val colIndex = tableShowArgs.colList.size * timelineIndex + colNameIndex + 1
                    val cellIndex = (colIndex + 65).toChar.toString + rowIndex.toString
                    //                    cellMap = cellMap ++ Map((displayName, timeline, colName) -> (cell(jobid, tableName, cellIndex, "", "Number", List(colCss, rowCss)), data2Value))
                    tableCells.noValueCells = tableCells.noValueCells ++
                            Map((displayName, timelineIndex.toString, colName) -> cell(cellIndex, "", "Number", List(colCss, rowCss), data2Value))
                }
            }
        }
        args
    }
}

case class phGetShowStackedTableBodyStyleAction() extends tableActionBase{
    override val name: String = "get show table body style"

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableShowArgs = args(argsMapKeys.TABLE_SHOW_ARGS).asInstanceOf[tableShowArgs]
        val tableCells = args(argsMapKeys.TABLE_CELLS).asInstanceOf[tableCells]
        val cityList = args(argsMapKeys.CITY).asInstanceOf[List[String]]
        tableShowArgs.rowList.zipWithIndex.foreach { case (displayNameAndCss, displayNameIndex) =>
            val rowIndex = displayNameIndex + 2
            val rowCss = displayNameAndCss._2
            val displayName = displayNameAndCss._1
            val displayNemeCellIndex = "A" + rowIndex.toString
            val rowTitleCss = tableShowArgs.rowTitle._2
            //            addCell(jobid, tableName, "A" + rowIndex.toString, displayName, "String", List(rowTitle._2, rowCss))
            tableCells.readyCells = tableCells.readyCells :+  s"#c#$displayNemeCellIndex#v#$displayName#t#String#s#$rowTitleCss*$rowCss"

            cityList.zipWithIndex.foreach { case (city, cityIndex) =>
                //                val timeline = timelineAndCss._1
                //                val timelineCss = timelineAndCss._2
                tableShowArgs.colList.zipWithIndex.foreach { case (colNameAndCss, colNameIndex) =>
                    val colName = tableShowArgs.col2DataColMap.getOrElse(colNameAndCss._1, colNameAndCss._1).replaceAll(" in[\\s\\S]*", "")
                    val data2ValueMap = args(argsMapKeys.DATA_2_Cell_VALUE_MAP).asInstanceOf[Map[String, String => String]]
                    val data2Value = data2ValueMap.getOrElse(colNameAndCss._1, data2ValueMap("DOT"))
                    val colCss = colNameAndCss._2
                    val colIndex = tableShowArgs.colList.size * cityIndex + colNameIndex + 1
                    val cellIndex = (colIndex + 65).toChar.toString + rowIndex.toString
                    //                    cellMap = cellMap ++ Map((displayName, timeline, colName) -> (cell(jobid, tableName, cellIndex, "", "Number", List(colCss, rowCss)), data2Value))
                    tableCells.noValueCells = tableCells.noValueCells ++
                            Map((displayName, city, colName) -> cell(cellIndex, "", "Number", List(colCss, rowCss), data2Value))
                }
            }
        }
        args
    }
}

case class phGetShowTrendsTableBodyValueAction() extends tableActionBase {
    override val name: String = "put data value into table body"

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableCells = args(argsMapKeys.TABLE_CELLS).asInstanceOf[tableCells]
        val rdd = args(argsMapKeys.DATA).asInstanceOf[RDD[(String, List[String])]]
        val resultMap = rdd.collect().toMap
        tableCells.noValueCells.foreach(x => {
            x._2.setValue(resultMap.getOrElse(x._1._1,List.fill(24)("0"))(x._1._2.toInt))
        })
        tableCells.allReady()
        args
    }
}

case class phGetShowCityStackedTableBodyValueAction() extends tableActionBase {
    override val name: String = "put data value into table body"

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val tableCells = args(argsMapKeys.TABLE_CELLS).asInstanceOf[tableCells]
        val dataFrame = args(argsMapKeys.DATA).asInstanceOf[DataFrame]
        val dataColNames = dataFrame.columns

        dataFrame.collect().foreach(x => {
            val row = x.toSeq.zip(dataColNames).toList
            val displayName = row.find(x => x._2.equals("DISPLAY_NAME")).get._1.toString
            val city = row.find(x => x._2.equals("CITY")).get._1.toString
            row.foreach(x => {
                val oneCell = tableCells.noValueCells.getOrElse((displayName, city, x._2),cell("","","",Nil))
                oneCell.setValue(x._1.toString)
            })
        })
        tableCells.allReady()
        args
    }
}

