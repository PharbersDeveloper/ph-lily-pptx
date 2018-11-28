package com.pharbers.process.stm.step.pptx.filter

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

trait phFilter{

}

class phSearchFilterImpl extends phFilter with phCommand {
    override def exec(args: Any): DataFrame = {
        val js = args.asInstanceOf[JsValue]
        val displayNames = "'" + (js \ "display").as[List[String]].reduce(_ + "','" + _) + "'"
        val timeline = (js \ "timeline").as[List[String]]
//        val filt = (js \ "filt").as[List[String]].reduce(_ + "," + _)
        def calcYM(start: Int, end: Int): List[Int] = {
            (end / 100 - start / 100) == 0 match {
                case true => (start to end).toList
                case _ => (start to (start / 100 * 100 + 12)).toList ::: calcYM(start / 100 * 100 + 101, end)
            }
        }
        val ym = calcYM(timeline(0).toInt, timeline(1).toInt)
                .map(x => x.toString.substring(4) + "/" + x.toString.substring(0, 4))
                .reduce(_ + "," + _)
        phLyFactory.getStorageWithName("gen_search_set")
            .where(s"'Display Name' in ($displayNames) and YM in ($ym)")
    }
}
