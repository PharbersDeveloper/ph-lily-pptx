package com.pharbers.process.read

import java.util.{Calendar, Date}

import com.pharbers.process.common.{phCommand, phLyDataSet, phLyFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait phReadData extends java.io.Serializable {
    val start : Int
    val step : Int
    val cat : Int
    val primary : List[Int]
    val seed = "#alfred#"
    val headline = 2

    def md5Hash(text : String):String = java.security.MessageDigest.
        getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map{"%02x".format(_)}.foldLeft(""){_+_}

    def genPrimaryId(text: String) : String = md5Hash(text)

    def formatDF(path: String) : String = {
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

            val Bline =
                if (row(this.primary.tail.head) == null) "null"
                else row(this.primary.tail.head).toString.replaceAll(" +", " ")

            if (Aline != "PRODUCT DESC") md5Hash(Aline + seed + Bline)
            else "PRODUCT ID"
        }.filter(row => row._1 != md5Hash("null#alfred#null")).map { row =>
            val inner =
                (start to (start + cat * step - 1)).map { idx =>

                    val offset = (idx - start) % step
                    val cal = Calendar.getInstance()
                    cal.setTime(dt)
                    cal.add(2 /*Month*/, offset)
                    val cal_str = dt_f.format(cal.getTime)

                    val tt = if (idx < start + (cat - 1) * step) cat01
                    else cat02

                    if (row._1  == "PRODUCT ID") row._1 -> phLyDataSet(row._2(this.primary.head).toString, row._2(this.primary.tail.head).toString, "DATE", "TYPE", 0)
                    else row._1 -> phLyDataSet(row._2(this.primary.head).toString, row._2(this.primary.tail.head).toString, cal_str, tt.toString, BigDecimal(row._2(idx).toString))
                }
            inner.toList
        }.flatMap(x => x).filter(_._1 != "PRODUCT ID")
//        rdd.distinct().take(200).foreach(println)
        println(rdd.count())

        val result = md5Hash(cat02.toString + new Date().getTime)
        phLyFactory.setStorageWithName(result, rdd)
        result
    }
}

//class phReadDataImpl extends phReadData with phCommand {
//    override def exec(args : Any) : Any = this.loadDataFromPathInHDFS(args.asInstanceOf[String])
//}
