package com.pharbers.process.merge

import com.pharbers.baseModules.PharbersInjectModule
import com.pharbers.process.common.{phCommand, phLyDataSet, phLyFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}

trait phMergeWithDot extends java.io.Serializable {
//    def getMainTable(dotPath: String, bigTableDFlst: List[DataFrame]): DataFrame = {
//        val tmp = phLyFactory.getCalcInstance()
//        val dotDF = tmp.readCsv(dotPath).withColumnRenamed("PRODUCT DESC", "PRODUCT DESC_config")
//            .withColumnRenamed("PACK DESC", "PACK DESC_config")
//            .filter(col("ADD RATE") =!= "N")
//        val bigTableDF: DataFrame = bigTableDFlst.reduce((totalDF, df) => totalDF union df)
//        val resultDF = bigTableDF.join(dotDF, bigTableDF("PRODUCT DESC") === dotDF("PRODUCT DESC_config") && bigTableDF("PACK DESC") === dotDF("PACK DESC_config"))
//            .withColumn("DOT", when(col("UNIT_TYPE") === "ST-CNT.UNIT", col("ST-CNT-UNIT") / col("ADD RATE"))
//                .otherwise(when(col("UNIT_TYPE") === "DT-DOS.UNIT", col("DT-DOS-UNIT") / col("ADD RATE"))
//                    .otherwise(when(col("UNIT_TYPE") === "UN-T.UNITST", col("UN-T-UNITS") / col("ADD RATE")))))
//        println("DOT计算完成=========================")
//        resultDF
//    }

}

class phMergeWithDotImpl extends phMergeWithDot with phCommand {
    var path = ""
    val primary = 1 :: 2 :: Nil
    val seed = "#alfred#"

    def md5Hash(text : String):String = java.security.MessageDigest.
        getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map{"%02x".format(_)}.foldLeft(""){_+_}

    def genPrimaryId(text: String) : String = md5Hash(text)

    override def preExec(args : Any) : Unit = path = args.asInstanceOf[String]

    override def exec(args: Any): Any = {
        /**
          * 1. 从storage中把数据拿出来
          */
        val rdd = phLyFactory.getStorageWithName("main frame")

        /**
          * 2. 从HDFS中读取
          */
        val tmp = phLyFactory.getCalcInstance()
        val df: DataFrame = tmp.ss.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", ",")
            .load(path)

        val rdd_add = df.toJavaRDD.rdd.keyBy { row =>
            val Aline =
                if (row(this.primary.head) == null) "null"
                else row(this.primary.head).toString.replaceAll(" +", " ")

            val Bline =
                if (row(this.primary.tail.head) == null) "null"
                else row(this.primary.tail.head).toString.replaceAll(" +", " ")

            if (Aline != "PRODUCT DESC") md5Hash(Aline + seed + Bline)
            else "PRODUCT ID"
        }.filter(row => row._1 != md5Hash("null#alfred#null") && row._1 != "PRODUCT ID").filter ( row => row._2(3) != "N")
        .map { row =>
            row._1 -> phLyDataSet(row._2(1).toString, row._2(2).toString, "", row._2(4).toString, 0, BigDecimal(row._2(3).toString))
        }

//        println(rdd_add.count)

        val reval = rdd.keyBy(row => row._1 + "===" + row._2.tp)
                .leftOuterJoin(rdd_add.keyBy(row => row._1 + "===" + row._2.tp))
                .map { iter =>
                    if (iter._2._2 != None) {
                        val addrate = iter._2._2.get._2.add_rate
                        iter._2._1._2.add_rate = addrate
                        iter._2._1._2.dot = iter._2._1._2.value / addrate
                    }
                    iter._2._1
                }

//        assert(rdd.count() == reval.count())
        phLyFactory.setStorageWithName("main frame dot", reval)
        phLyFactory.saveMidProcess("main frame dot", "hdfs:///test/mid/main-frame-with-dot/")
        println(reval.count)
        println(reval.filter(row => row._2.dot > 0).count)
        Some(reval)
    }
}
