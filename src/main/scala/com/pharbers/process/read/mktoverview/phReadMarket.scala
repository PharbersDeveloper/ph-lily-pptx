package com.pharbers.process.read.mktoverview

import java.util.Calendar

import com.pharbers.process.common.{phCommand, phLyFactory, phLyMktData}
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame

trait phReadMarket extends java.io.Serializable {
    val start: Int
    val step: Int
    val cat: Int
    val primary: List[Int]
    val seed = "#cui#"
    val headline = 2

    def md5Hash(text : String):String = java.security.MessageDigest.
        getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map{"%02x".format(_)}.foldLeft(""){_+_}

    def formatDF(path: String) : Unit = {
        val tmp = phLyFactory.getCalcInstance()
        val df: DataFrame = tmp.ss.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", ",")
            .load(path)
        val timeline = df.head().apply(start).toString.split("~").last.trim
        val dt_f = new java.text.SimpleDateFormat("MM/yyyy")
        val dt = dt_f.parse(timeline.toString)

        val rdd = df.toJavaRDD.rdd.keyBy { row =>
            val Aline =
                if (row(this.primary.head) == null) "null"
                else row(this.primary.head).toString.replaceAll(" +", " ")
            if (Aline != "MANUF.TYPE DESC") md5Hash(Aline + seed)
            else "MANUF_TYPE"
        }.filter(row => row._1 != md5Hash("null#cui#")).map { row =>
            val inner =
                (start to (start + cat * step - 1)).map { idx =>

                    val offset = (idx - start) % step
                    val cal = Calendar.getInstance()
                    cal.setTime(dt)
                    cal.add(2 /*Month*/, offset)
                    val cal_str = dt_f.format(cal.getTime)

                    if (row._1  == "MANUF_TYPE") row._1 -> phLyMktData(row._2(this.primary.head).toString, "DATE", 0)
                    else row._1 -> phLyMktData(row._2(this.primary.head).toString, cal_str, BigDecimal(row._2(idx).toString))
                }
            inner.toList
        }.flatMap(x => x).filter(_._1 != "MANUF_TYPE")
        lazy val sparkDriver: phSparkDriver = phSparkDriver("cui-test")

        import sparkDriver.ss.implicits._
        val resultDF = rdd.map(iter => (iter._2.manuf_type, iter._2.date, iter._2.value))
            .toDF("MANUF", "DATE", "VALUE")
        phLyFactory.setStorageWithDFName("market", resultDF)
    }
}

class phReadMarketImpl extends phReadMarket with phCommand {
    override val start = 2
    override val step = 60
    override val cat = 1
    override val primary = 1 :: Nil
    override def exec(args: Any): Any = this.formatDF(args.asInstanceOf[String])
}
