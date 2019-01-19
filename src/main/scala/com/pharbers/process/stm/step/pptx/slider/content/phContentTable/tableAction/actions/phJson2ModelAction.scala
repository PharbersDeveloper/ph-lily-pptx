package com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.actions

import com.pharbers.process.common.jsonData.{col, phTable, row}
import com.pharbers.process.stm.step.pptx.slider.content.phContentTable.tableAction.{argsMapKeys, tableActionBase}
import play.api.libs.json.{JsValue, Json}

case class phJson2ModelAction() extends tableActionBase{
    override val name: String = argsMapKeys.TABLE_MODEL

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val element = args("element").asInstanceOf[JsValue]
        implicit val row = Json.format[row]
        implicit val col = Json.format[col]
        implicit val phTable = Json.format[phTable]
        val table = element.as[phTable]
        args ++ Map(name -> table)
    }
}

case class phCityStackedJson2ModelAction() extends tableActionBase{
    override val name: String = argsMapKeys.TABLE_MODEL

    override def show(args: Map[String, Any]): Map[String, Any] = {
        val element = args("element").asInstanceOf[JsValue]
        implicit val row = Json.format[row]
        implicit val col = Json.format[col]
        implicit val phTable = Json.format[phTable]
        val table = element.as[phTable]
        val city = (element \ "city").as[List[String]]
        args ++ Map(name -> table, city -> argsMapKeys.CITY)
    }
}
