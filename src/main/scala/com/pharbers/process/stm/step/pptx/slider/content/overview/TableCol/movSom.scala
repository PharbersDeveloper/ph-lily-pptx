package com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol

import com.pharbers.process.common.phCommand
import com.pharbers.process.stm.step.pptx.slider.content.phReportTableCol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class movSom extends phCommand with phReportTableCol {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val data = argsMap("data").asInstanceOf[DataFrame]
        val timelineList = argsMap("timelineList").asInstanceOf[List[String]]
        timelineList.map { timleline =>
            val mktDisplayNameSum = data.filter(col("TIMELINE") === timleline)
                .filter(col("DISPLAY_NAME") === "CHPA Total Mkt")
                .collect().head.getDecimal(3)
            data.filter(col("TIMELINE") === timleline)
                .withColumn("SOM", col("RESULT") / mktDisplayNameSum)
        }.reduce((df1, df2) => df1.union(df2))
    }
}
