package com.pharbers.process.read

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.{DataFrame}
import java.util.{Calendar}

trait phReadDataFourTitle extends java.io.Serializable {
    def md5Hash(text : String):String = java.security.MessageDigest.
        getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map{"%02x".format(_)}.foldLeft(""){_+_}

    def genPrimaryId(text: String) : String = md5Hash(text)

    val start = 4
    val step = 60
    val cat = 2
    val seed = "#alfred#"
    def formatDF(path: String): DataFrame = {
        val tmp = phLyFactory.getCalcInstance()
        val df: DataFrame = tmp.ss.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", ",")
            .load(path)
        val timeline = df.head().apply(4).toString.split("~").last.trim
        val dt_f = new java.text.SimpleDateFormat("MM/yyyy")
        val dt = dt_f.parse(timeline.toString)

        val cat01 = df.head(2).last.apply(start)
        val cat02 = df.head(2).last.apply(start + step)

        val rdd = df.toJavaRDD.rdd.keyBy { row =>
            val Aline =
                if (row(1) == null) "null"
                else row(1).toString.replaceAll(" +", " ")

            val Bline =
                if (row(3) == null) "null"
                else row(3).toString.replaceAll(" +", " ")

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

                    if (row._1  == "PRODUCT ID") row._1 -> (row._2(1), row._2(3), "DATE", "TYPE", "VALUE")
                    else row._1 -> (row._2(1), row._2(3), cal_str, tt, row._2(idx))
                }
            inner.toList
        }.flatMap(x => x).filter(_._1 != "PRODUCT ID")
//        rdd.distinct().filter(_._2._4.toString != "LC_RMB").take(200).foreach(println)
        println(rdd.count())

        df
    }
}

class phReadDataFourTitleImpl extends phReadDataFourTitle with phCommand {
    override def exec(args : Any) : Any = this.formatDF(args.asInstanceOf[String])
}