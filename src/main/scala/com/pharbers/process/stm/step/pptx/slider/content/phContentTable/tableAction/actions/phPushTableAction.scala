package com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions

import java.util.UUID

import com.pharbers.phsocket.phSocketDriver
import com.pharbers.process.common.DTO.tableShowArgs
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{argsMapKeys, tableActionBase}

case class phPushTableAction() extends tableActionBase{

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val cells = args(argsMapKeys.SHOW_TABLE).asInstanceOf[List[String]]
        val jobId = args(argsMapKeys.JOB_ID).toString
        val pos = args(argsMapKeys.TABLE_SHOW_ARGS).asInstanceOf[tableShowArgs].pos
        val sliderIndex = args(argsMapKeys.SLIDE_INDEX).asInstanceOf[Int]
        val tableName = UUID.randomUUID().toString
        val socketDriver = phSocketDriver()
        cells.sliding(30, 30).foreach(x => {
            socketDriver.setExcel(jobId, tableName, x)
        })
        Thread.sleep(3000)
        socketDriver.excel2PPT(jobId, tableName, pos, sliderIndex)
        args
    }
}
