package com.pharbers.process.read.mktoverview

import java.util.Calendar
import com.pharbers.process.common.{phCommand, phLyFactory, phLyProdData}
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame

trait phReadLLYProd extends java.io.Serializable {
    val start : Int
    val step : Int
    val cat : Int
    val primary : List[Int]
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

        val cat01 = df.head(headline).last.apply(start)
        val cat02 = df.head(headline).last.apply(start + step)

        val rdd = df.toJavaRDD.rdd.keyBy { row =>
            val Aline =
                if (row(this.primary.head) == null) "null"
                else row(this.primary.head).toString.replaceAll(" +", " ")

            if (Aline != "PRODUCT DESC") md5Hash(Aline + seed)
            else "PRODUCT ID"
        }.filter(row => row._1 != md5Hash("null#cui#")).map { row =>
            val inner =
                (start to (start + cat * step - 1)).map { idx =>

                    val offset = (idx - start) % step
                    val cal = Calendar.getInstance()
                    cal.setTime(dt)
                    cal.add(2 /*Month*/, offset)
                    val cal_str = dt_f.format(cal.getTime)

                    val tt = if (idx < start + (cat - 1) * step) cat01
                    else cat02

                    if (row._1  == "PRODUCT ID") row._1 -> phLyProdData(row._2(this.primary.head).toString, "DATE", "TYPE", 0)
                    else row._1 -> phLyProdData(row._2(this.primary.head).toString, cal_str, tt.toString, BigDecimal(row._2(idx).toString))
                }
            inner.toList
        }.flatMap(x => x).filter(_._1 != "PRODUCT ID")
        lazy val sparkDriver: phSparkDriver = phSparkDriver("cui-test")

        import sparkDriver.ss.implicits._
        val resultDF = rdd.map(iter => (iter._2.product, iter._2.date, iter._2.tp, iter._2.value))
            .toDF("PRODUCT", "DATE", "TYPE", "VALUE")
        phLyFactory.setStorageWithDFName("LLYProd", resultDF)
    }
}


class phReadLLYProdImpl extends phReadLLYProd with phCommand {
    override val start = 4
    override val step = 60
    override val cat = 2
    override val primary = 3 :: Nil
    override def exec(args: Any): Any = this.formatDF(args.asInstanceOf[String])
}

