package com.pharbers.process.common.jsonData

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

case class phTable(var factory: String, var mkt_display: String, var mkt_col: String, var pos: List[Int], var timeline: List[String], var col: col,  var row: row){
    def initOne (list: List[String], Table2DataMap: Map[Any, Any])(func: String => Any): Any ={
        list.map(x => Table2DataMap.getOrElse(func(x),func(x)))
    }


}

case class col(var title: String, var count: List[String])

case class row(var title: String, var display_name: List[String])

object phTable2Data{
    val jsonCol2DataColMap: Map[Any, Any] = Map(

        "DOT(Mn)" -> "dot",
        "MMU" -> "dot",
        "Tablet" -> "dot",
        "RMB" -> "LC-RMB",
        "RMB(Mn)" -> "LC-RMB",
        "LC-RMB" -> "LC-RMB",
        "DOT" -> "dot",
        "Mg(Mn)" -> "dot",
        "MG(Mn)" -> "dot",
        "RMB(Mn)" -> "LC-RMB",
        "SOM(%)" -> "som",
        "SOM" -> "som",
        "SOM%" -> "som",
        "Share" -> "som",
        "Growth(%)" -> "Growth(%)",
        "GROWTH(%)" -> "Growth(%)",
        "YoY GR(%)" -> "Growth(%)",
        "YoY GR" -> "Growth(%)",
        "YOY GR" -> "Growth(%)",
        "GR(%)" -> "Growth(%)",
        "Growth Contribution%" -> "GrowthContribution%",
        "SOM in Branded MKT(%)" -> "som",
        "SOM in Branded MKT%" -> "som")

    val jsonShowCol2DataColMap: String => Map[String, String] = mktDisplayName =>
        Map("DOT(Mn)" -> "RESULT",
            "MMU" -> "RESULT",
            "Tablet" -> "RESULT",
            "SOM(%)" -> ("SOM in " + mktDisplayName),
            "SOM" -> ("SOM in " + mktDisplayName),
            "SOM%" -> ("SOM in " + mktDisplayName),
            "Share" -> ("SOM in " + mktDisplayName),
            "Growth(%)" -> "GROWTH",
            "GROWTH(%)" -> "GROWTH",
            "YoY GR(%)" -> "GROWTH",
            "YoY GR" -> "GROWTH",
            "YOY GR" -> "GROWTH",
            "GR(%)" -> "GROWTH",
            "Growth Contribution%" -> "GROWTH_CONTRIBUTION",
            "RMB" -> "RESULT",
            "RMB(Mn)" -> "RESULT",
            "RMB(Bn)" -> "RESULT",
            "DOT" -> "RESULT",
            "SOM in Branded MKT(%)" -> ("SOM in " + mktDisplayName),
            "SOM in Branded MKT%" -> ("SOM in " + mktDisplayName),
            "Mg(Mn)" -> "RESULT",
            "MG(Mn)" -> "RESULT",
            "RMB(Mn)" -> "RESULT")

    val removeCssAndSomething: String => String => String = something => str => str.split(":").head.replace(something, "")

    val quarter2timeLine: Date => String => String = start => quarter => {
        val cal = Calendar.getInstance()
        cal.setTime(start)
        cal.add(Calendar.MONTH, "-?\\d+".r.findFirstIn("#qtime-?\\d+#".r.findFirstIn(quarter).get).getOrElse("8").toInt * 3 - 24)
        quarter.replaceAll("#qtime-?\\d+#", new SimpleDateFormat("MM yy").format(cal.getTime))
    }

    val severCss: String => (String, String) = str =>  (str.split(":").head, str.split(":").tail.headOption.getOrElse(""))

    val Month2timeLine: Date => String => String = start => quarter => {
        val cal = Calendar.getInstance()
        cal.setTime(start)
        cal.add(Calendar.MONTH, "-?\\d+".r.findFirstIn("#time-?\\d+#".r.findFirstIn(quarter).get).getOrElse("24").toInt - 24)
        quarter.replaceAll("#time-?\\d+#", new SimpleDateFormat("MM yy").format(cal.getTime))
    }
}