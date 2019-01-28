package com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions

import com.pharbers.process.common.jsonData._
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{argsMapKeys, tableActionBase}
import play.api.libs.json._
import play.api.libs.functional.syntax._

trait phJson2Model {
    implicit val row = Json.format[row]
    implicit val col = Json.format[col]
    implicit val showDis = Json.format[phShowDisplayName]
    implicit val tableFormat: Format[phTable] = ((__ \ "factory").format[String] and (__ \ "mkt_display").format[String] and
            (__ \ "mkt_col").format[String] and (__ \ "pos").format[List[Int]] and (__ \ "timeline").format[List[String]] and
            (__ \ "col").format[col] and (__ \ "row").format[row] and
            (__ \ "show_display").formatNullable[List[phShowDisplayName]].inmap[List[phShowDisplayName]](o => o.getOrElse(Nil), s => Some(s)))(phTable.apply, unlift(phTable.unapply))


}

case class phJson2ModelAction() extends tableActionBase with phJson2Model{
    override val name: String = argsMapKeys.TABLE_MODEL

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val element = args("element").asInstanceOf[JsValue]
        val table = element.as[phTable]
        args ++ Map(name -> table)
    }
}

case class phCityStackedJson2ModelAction() extends tableActionBase with phJson2Model{
    override val name: String = argsMapKeys.TABLE_MODEL

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val element = args("element").asInstanceOf[JsValue]
        val table = element.as[phTable]
        val city = (element \ "city").as[List[String]]
        args ++ Map(name -> table, argsMapKeys.CITY -> city)
    }
}

//case class phShowDisplayJson2ModelAction() extends tableActionBase{
//    override val name: String = argsMapKeys.TABLE_MODEL
//
//    override def show(args: Map[String, Any]): Map[String, Any] = {
//        val element = args("element").asInstanceOf[JsValue]
//        implicit val row = Json.format[row]
//        implicit val col = Json.format[col]
//        implicit val phTable = Json.format[phTable]
//        implicit val showDis = Json.format[phShowDisplayName]
//        val table = element.as[phTable]
//        val showDis = (element \ "show_display").as[List[phShowDisplayName]]
//        args ++ Map(name -> table)
//    }
//}
