package com.pharbers

import com.pharbers.process.common.{phCommand, phLyFactory}

object main extends App {
    phLyFactory.startProcess
//    phLyFactory.setSaveMidDoc
    phLyFactory.getInstance("com.pharbers.process.flow.phBIFlowGenImpl").asInstanceOf[phCommand].exec(null)
    phLyFactory.endProcess
}

object test extends App {
    val driver = phLyFactory.getCalcInstance()
    driver.sc.setLogLevel("ERROR")
    val bigTable = driver.readCsv("/test/result03")
    phLyFactory.stssoo += ("gen_data_set" -> bigTable)
    phLyFactory.getInstance("com.pharbers.process.stm.step.gen.genSearchSet.phGenSearchSetImpl").asInstanceOf[phCommand].exec(null)
    phLyFactory.getInstance("com.pharbers.process.stm.step.pptx.phGenPPTImpl").asInstanceOf[phCommand].exec(null)
}
