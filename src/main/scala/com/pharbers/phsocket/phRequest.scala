package com.pharbers.phsocket

import java.io.DataOutputStream
import java.util.UUID

import com.pharbers.pptxmoudles.{PhExcel2PPT, PhExcelPush, PhRequest, PhTextPPT}
import com.pharbers.macros._
import com.pharbers.macros.convert.jsonapi.JsonapiMacro._
import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport
import io.circe.syntax._

trait phSocket_managers extends createPPT with setExcel with excel2PPT with createTitle

sealed trait phRequest extends phSocket_trait {
    val dataOutputStream = new DataOutputStream(socket.getOutputStream)
    def sendMessage(msg: String): Unit ={
        Thread.sleep(1000)
        dataOutputStream.writeUTF(msg)
        dataOutputStream.flush()
    }
}

trait createPPT extends phRequest with CirceJsonapiSupport{
    def createPPT(jobid: String): Unit ={
        val id: String = UUID.randomUUID().toString
        val request = new PhRequest
        request.id = id
        request.`type` = "PhRequest"
        request.command = "GenPPT"
        request.jobid = jobid
        val msg = toJsonapi(request).asJson.toString()
        println(msg)
        sendMessage(msg)
    }
}

trait setExcel extends phRequest with CirceJsonapiSupport{
    def setExcel(jobid: String, excelName: String, cell: String, value: String, cate: String): Unit = {
        val id: String = UUID.randomUUID().toString
        val request = new PhRequest
        request.id = id
        request.`type` = "PhRequest"
        request.command = "ExcelPush"
        request.jobid = jobid
        val phExcelPush = new PhExcelPush
        phExcelPush.id = UUID.randomUUID().toString
        phExcelPush.`type` = "PhExcelPush"
        phExcelPush.name = excelName
        phExcelPush.cell = cell
        phExcelPush.cate = cate    //Number or String
        phExcelPush.value = value
        request.push = Some(phExcelPush)
        val msg = toJsonapi(request).asJson.toString()
        sendMessage(msg)
    }
}

trait excel2PPT extends phRequest with CirceJsonapiSupport{
    def excel2PPT(jobid: String, excelName: String, pos: List[Int], sliderIndex: Int): Unit ={
        val id: String = UUID.randomUUID().toString
        val request = new PhRequest
        request.id = id
        request.`type` = "PhRequest"
        request.command = "Excel2PPT"
        request.jobid = jobid
        val phExcel2PPT = new PhExcel2PPT
        phExcel2PPT.id = UUID.randomUUID().toString
        phExcel2PPT.`type` = "Excel2PPT"
        phExcel2PPT.name = excelName
        phExcel2PPT.pos = pos
        phExcel2PPT.slider = sliderIndex
        request.e2p = Some(phExcel2PPT)
        val msg = toJsonapi(request).asJson.toString()
        sendMessage(msg)
    }
}

trait createTitle extends phRequest with CirceJsonapiSupport{
    def createTitle(jobid: String, content: String, pos: List[Int], slider: Int, css: String): Unit ={
        val id: String = UUID.randomUUID().toString
        val request = new PhRequest
        request.id = id
        request.`type` = "PhRequest"
        request.command = "PushText"
        request.jobid = jobid
        val phTest2PPT = new PhTextPPT
        phTest2PPT.`type` = "PhTextSetContent"
        phTest2PPT.content = content
        phTest2PPT.pos = pos
        phTest2PPT.slider = slider
        phTest2PPT.css = css
        request.text = Some(phTest2PPT)
        val msg = toJsonapi(request).asJson.toString()
        sendMessage(msg)
    }
}
