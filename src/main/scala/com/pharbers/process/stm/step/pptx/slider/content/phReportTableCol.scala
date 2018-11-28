package com.pharbers.process.stm.step.pptx.slider.content

import com.pharbers.process.common.{phCommand, phLyFactory}
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


trait phReportTableCol{
    lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
    import sparkDriver.ss.implicits._

    var data: DataFrame = _
    def getYmDF(ymstr: String): DataFrame = {
        val p1 = "MAT M[0-9][0-9] [0-9][0-9]".r
        val ym = ymstr.substring(5).split(" ")
        val month = ym(0).toInt
        val year = 2000 + ym(1).toInt
        getymlst(List(month.toString + "/" + year.toString), month, year, 12).map { str =>
            if (str.length == 7) str
            else "0" + str
        }.toDF("yms")
    }
    def getymlst(ymlst: List[String], month: Int, year: Int, ymcount: Int): List[String] = {
        if (ymlst.size == ymcount) {
            ymlst
        } else {
            if (month == 1) getymlst(ymlst ::: List(month + "/" + year), 12, year - 1, ymcount)
            else getymlst(ymlst ::: List(month + "/" + year), month - 1, year, ymcount)
        }
    }
}

class dotMn extends  phReportTableCol with phCommand {
    override def exec(args: Any): Any = {
        val argMap = args.asInstanceOf[Map[String, Any]]
        data = argMap("data").asInstanceOf[DataFrame]
        val displayName = argMap("displayName").asInstanceOf[String]
        val ym = argMap("ym").asInstanceOf[String]
        val ymDF = getYmDF(ym)
        val result = data.filter(col("Display name") === displayName)
                .join(ymDF, data("YM") === ymDF("yms"))
                .select("DOT")
                .agg(Map("DOT" -> "sum"))
                .collectAsList().get(0).toString()
        (result.substring(1, result.length - 1).toDouble / 1000000).toString
    }
}
