package com.pharbers.process.stm.step.pptx.slider.content

import com.pharbers.process.common.phCommand
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait phReportTableCol{
    var data: DataFrame = _
}

class dotMn extends  phReportTableCol with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        data = argMap("data").asInstanceOf[DataFrame]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ymDF = argMap("ym").asInstanceOf[DataFrame]
        val result = data.filter(col("Display name") === displayName)
                .join(ymDF, data("YM") === ymDF("yms"))
                .select("DOT")
                .agg(Map("DOT" -> "sum"))
                .collectAsList().get(0).toString()
        (result.substring(1, result.length - 1).toDouble / 1000000).toString
    }
}
