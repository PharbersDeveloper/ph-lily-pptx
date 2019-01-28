package com.pharbers.process.read.mktoverview

import java.util.Calendar

import com.pharbers.process.common.{phCommand, phLyFactory, phLyGLPData}
import com.pharbers.spark.phSparkDriver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

trait phReadGLP extends java.io.Serializable {
    val start: Int
    val step: Int
    val cat: Int
    val primary: List[Int]
    val seed = "#alfred#"
    val headline = 2

    def md5Hash(text: String): String = java.security.MessageDigest.
        getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
        "%02x".format(_)
    }.foldLeft("") {
        _ + _
    }

    def genPrimaryId(text: String): String = md5Hash(text)

    def formatDF(path: String, mapKey: String): Unit = {
        val tmp = phLyFactory.getCalcInstance()
        val df: DataFrame = tmp.ss.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", ",")
            .load(path)
        val timeline = df.head().apply(start).toString.split("~").last.trim
        val monthType = df.head().apply(start).toString.split("~").head.trim
        val dt_f = new java.text.SimpleDateFormat("MM/yyyy")
        val dt = dt_f.parse(timeline.toString)

        val funcAll : Int => Int = offset => offset
        val funcCity : Int => Int = offset => offset * 3

        val offsetMap = Map("QTR" -> funcCity, "MTH" -> funcAll)

        val cat01 = df.head(headline).last.apply(start)
        val cat02 = df.head(headline).last.apply(start + step)

        val keyCount = this.primary.size
        val func_city: Row => String = row => {
            row(this.primary.last).toString.replace("Hospital Audit", "").replace("Delta", "").trim.toLowerCase()
        }
        val func_all: Row => String = row => "default"
        val funcMap = Map(2 -> func_all, 3 -> func_city)

        val rdd = df.toJavaRDD.rdd.keyBy { row =>
            val Aline =
                if (row(this.primary.head) == null) "null"
                else row(this.primary.head).toString.replaceAll(" +", " ")

            val Bline =
                if (row(this.primary.tail.head) == null) "null"
                else row(this.primary.tail.head).toString.replaceAll(" +", " ")

            if (Aline != "TC II SHORT DESC") md5Hash(Aline + seed + Bline)
            else "TC II"
        }.filter(row => row._1 != md5Hash("null#alfred#null")).map { row =>
            val inner =
                (start to (start + cat * step - 1)).map { idx =>

                    val offset = (idx - start) % step
                    val cal = Calendar.getInstance()
                    cal.setTime(dt)
                    //                    cal.add(2 /*Month*/ , offset)
                    cal.add(2 /*Month*/ , offsetMap(monthType)(offset))
                    val cal_str = dt_f.format(cal.getTime)
                    val tt = if (idx < start + (cat - 1) * step) cat01
                    else cat02

                    if (row._1 == "TC II") row._1 -> phLyGLPData(row._2(this.primary.head).toString, row._2(this.primary.tail.head).toString, "DATE", "TYPE", 0)
                    else row._1 -> phLyGLPData(row._2(this.primary.head).toString, row._2(this.primary.tail.head).toString, cal_str, tt.toString, BigDecimal(row._2(idx).toString),
                        city = funcMap(keyCount)(row._2))
                }
            inner.toList
        }.flatMap(x => x).filter(_._1 != "TC II")
        lazy val sparkDriver: phSparkDriver = phSparkDriver("cui-test")

        import sparkDriver.ss.implicits._
        val resultDF = rdd.map(x => (x._1, x._2.TC_II_SHORT, x._2.TC_IV_SHORT, x._2.date, x._2.tp, x._2.add_rate, x._2.dot, x._2.value, x._2.city))
            .toDF("ID", "TC_II", "TC_IV", "DATE", "TYPE", "ADD RATE", "DOT", "VALUE", "CITY")
        phLyFactory.setStorageWithDFName(mapKey, resultDF)
    }
}

class phReadGLPCountryImpl extends phReadGLP with phCommand {
    override val start = 4
    override val step = 60
    override val cat = 2
    override val primary = 0 :: 2 :: Nil
    override def exec(args: Any): Any = this.formatDF(args.asInstanceOf[String], "GLP_country")
}

class phReadGLPCityImpl extends phReadGLP with phCommand {
    override val start = 6
    override val step = 20
    override val cat = 2
    override val primary = 2 :: 4 :: 1 :: Nil
    override def exec(args: Any): Any = this.formatDF(args.asInstanceOf[String], "GLP_city")
}