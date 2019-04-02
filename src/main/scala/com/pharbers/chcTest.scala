package com.pharbers

import com.pharbers.phsocket.phSocketDriver
import com.pharbers.process.common.phCommand
import play.api.libs.json.JsValue

class chcTest extends phCommand{
    override def exec(args: Any): Any = {
        val socketDriver = phSocketDriver()
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val element = argsMap("element").asInstanceOf[JsValue]
        val pos = (element \ "pos").as[List[Int]].map(x => (x / 0.000278).toInt)
        val slideIndex = argsMap("slideIndex").asInstanceOf[Int]
        val jobid = argsMap("jobid").asInstanceOf[String]
        Thread.sleep(3000)
        socketDriver.excel2Chart(jobid, (element \ "name").as[String], pos, slideIndex, (element \ "type").as[String])
    }
}

class chcTest2 extends phCommand{
    override def exec(args: Any): Any = {
        val socketDriver = phSocketDriver()
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val element = argsMap("element").asInstanceOf[JsValue]
        val pos = (element \ "pos").as[List[Int]].map(x => (x / 0.000278).toInt)
        val slideIndex = argsMap("slideIndex").asInstanceOf[Int]
        val jobid = argsMap("jobid").asInstanceOf[String]
        Thread.sleep(3000)
        socketDriver.excel2PPT(jobid, (element \ "name").as[String], pos, slideIndex)
    }
}

