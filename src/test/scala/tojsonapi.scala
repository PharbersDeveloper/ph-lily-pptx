import java.io.DataOutputStream
import java.util.regex.Pattern

import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport
import com.pharbers.pptxmoudles._
import com.pharbers.phsocket.phSocketDriver

object tojsonapi extends App with CirceJsonapiSupport {
    val socket = phSocketDriver().socket
    val dataOutputStream = new DataOutputStream(socket.getOutputStream)

    val request1 = new PhRequest
    request1.id = "1"
    request1.`type` = "PhRequest"
    request1.command = "GenPPT"
    request1.jobid = "cuiTest"

    //    val request2 = new PhRequest
    //    request2.id = "1"
    //    request2.`type` = "PhRequest"
    //    request2.command = "ExcelPush"
    //    request2.jobid = "cuiTest"
    //    val phExcelPush2 = new PhExcelPush
    //    phExcelPush2.id = "01"
    //    phExcelPush2.`type` = "PhExcelPush"
    //    phExcelPush2.name = "inner_xls"
    //    phExcelPush2.cell = "B2"
    //    phExcelPush2.cate = "Number"
    //    phExcelPush2.value = "123456"
    //    request2.push = Some(phExcelPush2)
    //
    //    val request3 = new PhRequest
    //    request3.id = "1"
    //    request3.`type` = "PhRequest"
    //    request3.command = "ExcelPush"
    //    request3.jobid = "cuiTest"
    //    val phExcelPush3 = new PhExcelPush
    //    phExcelPush3.id = "01"
    //    phExcelPush3.`type` = "PhExcelPush"
    //    phExcelPush3.name = "inner_xls"
    //    phExcelPush3.cell = "C4:D6"
    //    phExcelPush3.cate = "String"
    //    phExcelPush3.value = "CUI TEST"
    //    request3.push = Some(phExcelPush3)
    //
    //    val request4 = new PhRequest
    //    request4.id = "1"
    //    request4.`type` = "PhRequest"
    //    request4.command = "Excel2PPT"
    //    request4.jobid = "cuiTest"
    //    val phExcel2PPT4 = new PhExcel2PPT
    //    phExcel2PPT4.id = "01"
    //    phExcel2PPT4.`type` = "Excel2PPT"
    //    phExcel2PPT4.name = "inner_xls"
    //    phExcel2PPT4.pos = List(60, 60)
    //    phExcel2PPT4.slider = 1
    //    request4.e2p = Some(phExcel2PPT4)

    val request5 = new PhRequest
    request5.id = "1"
    request5.`type` = "PhRequest"
    request5.command = "PushText"
    request5.jobid = "cuiTest"
    val phTest2PPT5 = new PhTextPPT
    phTest2PPT5.`type` = "PhTextSetContent"
    phTest2PPT5.content = "cuiTest"
    phTest2PPT5.pos = List(10, 10, 700, 50)
    phTest2PPT5.slider = 0
    phTest2PPT5.css = "test"
    request5.text = Some(phTest2PPT5)

    //    try {

    //    import io.circe._

    //        import io.circe.generic.auto._

    import com.pharbers.macros._
    import com.pharbers.macros.convert.jsonapi.JsonapiMacro._
    import io.circe.syntax._


        Thread.sleep(1000)
    //    val str1 = getstr(toJsonapi(request1).asJson.toString())
    //    val str01 = new String(str1.getBytes("utf-8"), "utf-8")
        val str01 = toJsonapi(request1).asJson.toString()
        dataOutputStream.writeUTF(str01)
        dataOutputStream.flush()
        println(str01)

    //    Thread.sleep(1000)
    //    //    val str2 = getstr(toJsonapi(request2).asJson.toString())
    //    //    val str02 = new String(str2.getBytes("utf-8"), "utf-8")
    //    val str02 = toJsonapi(request2).asJson.toString()
    //    dataOutputStream.writeUTF(str02)
    //    dataOutputStream.flush()
    //    println(str02)

    //    Thread.sleep(1000)
    ////    val str3 = getstr(toJsonapi(request3).asJson.toString())
    ////    val str03 = new String(str3.getBytes("utf-8"), "utf-8")
    //    val str03 = toJsonapi(request3).asJson.toString()
    //    dataOutputStream.writeUTF(str03)
    //    dataOutputStream.flush()
    //    println(str03)

    //    Thread.sleep(1000)
    ////    val str4 = getstr(toJsonapi(request4).asJson.toString())
    ////    val str04 = new String(str4.getBytes("utf-8"), "utf-8")
    //    val str04 = toJsonapi(request4).asJson.toString()
    //    dataOutputStream.writeUTF(str04)
    //    dataOutputStream.flush()
    //    println(str04)

    Thread.sleep(1000)
    val str05 = toJsonapi(request5).asJson.toString()
    dataOutputStream.writeUTF(str05)
    dataOutputStream.flush()
    println(str05)

    Thread.sleep(1000)
    dataOutputStream.close()
    socket.close()
    //    } catch {
    //        case e => println(e)
    //    }

    //    val request = new PhRequest
    //    val jobid = UUID.randomUUID().toString
    //    request.id = ""
    //    request.jobid = jobid
    //    var phexcel2ppt = new PhExcel2PPT
    //    request.PhExcel2PPT = Some(phexcel2ppt)
    //    toJsonapi(request)
}
