package com.pharbers

import java.util.Date

import com.pharbers.phsocket.phSocketDriver
import com.pharbers.process.common.{phCommand, phLyFactory}
import com.pharbers.process.stm.step.pptx.slider.content.phReportContentTable

object main extends App {
    println(new Date())
    phReportContentTable.initTimeline("09 18")
    val jobid = phLyFactory.startProcess
    println("jobid:" + jobid)
    val socketDriver = phSocketDriver()
    socketDriver.createPPT(jobid)
    //    phLyFactory.setSaveMidDoc
    phLyFactory.getInstance("com.pharbers.process.flow.phBIFlowGenImpl").asInstanceOf[phCommand].exec(jobid)
    phLyFactory.endProcess
    println(new Date())
    println("jobid:" + jobid)
    Thread.sleep(5000)
}

object test extends App {

}

