package com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol

import com.pharbers.process.common.phCommand
import com.pharbers.process.stm.step.pptx.slider.content.phReportTableCol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class ev extends phCommand with phReportTableCol {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val mktDisplayName = argsMap("mktDisplayName").asInstanceOf[String]
        val data = argsMap("data").asInstanceOf[DataFrame]
        val totalResult = data.filter(col("DISPLAY_NAME") === mktDisplayName)
            .select("GROWTH")
            .collect().head.toString()
            .replaceAll("[\\[\\]]", "")
        data.withColumn("EV", ((col("GROWTH") + 100) / (BigDecimal(totalResult) + 100)) * 100)
    }
}
