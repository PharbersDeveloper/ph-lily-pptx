//import com.pharbers.pptxmoudles.PhRequest
//import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport
//
//object jsontest extends App  with CirceJsonapiSupport{
//    val request1 = new PhRequest
//    request1.id = "1"
//    request1.`type` = "PhRequest"
//    request1.command = "GenPPT"
//    request1.jobid = "cuiTest"
//    import com.pharbers.macros._
//    import com.pharbers.macros.convert.jsonapi.JsonapiMacro._
//    import io.circe.syntax._
//
//    val str = toJsonapi(request1).asJson.toString()
//    str.replace("/n", "").replace("/")
//    println(str)
//
//}
