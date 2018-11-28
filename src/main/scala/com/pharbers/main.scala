package com.pharbers

import com.pharbers.process.common.{phCommand, phLyFactory}

object main extends App {
    phLyFactory.getInstance("com.pharbers.process.flow.phBIFlowGenImpl").asInstanceOf[phCommand].exec(null)
}
