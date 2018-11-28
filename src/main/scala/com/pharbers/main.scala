package com.pharbers

import com.pharbers.process.common.{phCommand, phLyFactory}

object main extends App {
    phLyFactory.getInstance("com.pharbers.process.flow.phBIFlowGenImpl").asInstanceOf[phCommand].exec(null)
}

object test extends App {
    val search = phLyFactory.getCalcInstance().readCsv("/data/lily/source_file_20181118/fourTitle")
    val result = phLyFactory.getCalcInstance().readCsv("/data/lily/source_file_20181118/fourTitle")
}