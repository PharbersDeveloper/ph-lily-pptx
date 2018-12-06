//import java.io.DataOutputStream
//import java.util.UUID
//
//import akka.japi.Option.Some
//import com.pharbers.pptxmoudles
//import com.pharbers.pptxmoudles.PhRequest
//import com.pharbers.pptxmoudles.{PhExcel2PPT, PhExcelPush, PhExportPPT}
//import com.pharbers.macros.toJsonapi
//import com.pharbers.socket.phSocket
//import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport
//
//object moudlesTest extends App with CirceJsonapiSupport {
//    val socket = phSocket.socket
//    val dataOutputStream = new DataOutputStream(socket.getOutputStream)
//    //request1
//    val request1 = new PhRequest
//    request1.id = "1"
//    request1.`type` = "PhRequest"
//    request1.command = "GenPPT"
//    request1.jobid = "cuiTest"
//
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
//
//
//    //    try {
//
//    //    import io.circe._
//
//    //        import io.circe.generic.auto._
//
//    import com.pharbers.macros._
//    import com.pharbers.macros.convert.jsonapi.JsonapiMacro._
//    import io.circe.syntax._
//
//    Thread.sleep(1000)
//    val str = toJsonapi(new PhRequest).asJson.toString
//    dataOutputStream.writeUTF(str)
//    dataOutputStream.flush()
//
//    Thread.sleep(1000)
//    dataOutputStream.writeUTF(toJsonapi(request2).asJson.toString())
//    dataOutputStream.flush()
//
//    Thread.sleep(1000)
//    dataOutputStream.writeUTF(toJsonapi(request3).asJson.toString())
//    dataOutputStream.flush()
//
//    Thread.sleep(1000)
//    dataOutputStream.writeUTF(toJsonapi(request4).asJson.toString())
//    dataOutputStream.flush()
//
//    Thread.sleep(1000)
//    dataOutputStream.close()
//    socket.close()
//    //    } catch {
//    //        case e => println(e)
//    //    }
//
//    //    val request = new PhRequest
//    //    val jobid = UUID.randomUUID().toString
//    //    request.id = ""
//    //    request.jobid = jobid
//    //    var phexcel2ppt = new PhExcel2PPT
//    //    request.PhExcel2PPT = Some(phexcel2ppt)
//    //    toJsonapi(request)
//}
