package com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol

import com.pharbers.process.common.{phCommand, phLyFactory, phLyMOVData, phLycalData}
import com.pharbers.process.stm.step.pptx.slider.content.{growth, phReportTableCol}
import org.apache.spark.sql.functions.{col, when}
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame

class mixValue extends phCommand with phReportTableCol {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val data: DataFrame = argsMap("data").asInstanceOf[DataFrame]
        val mapData: DataFrame = argsMap("mapData").asInstanceOf[DataFrame]
        //List((DisplayName,TYPE))
        val displayNamelAndType: List[(String, String)] = argsMap("displayNameList").asInstanceOf[List[(String, String)]]
        val colList: List[String] = argsMap("colList").asInstanceOf[List[String]]
        val mktDisplayName: String = argsMap("mktDisplayName").asInstanceOf[String]
        //单个Timeline
        val timelineList: List[String] = argsMap("timelineList").asInstanceOf[List[String]]
        val allTimelineList: List[String] = if (colList.contains("Growth(%)")) {
            getAllTimeline(timelineList)
        } else {
            timelineList
        }
        val allTimelst: List[String] = allTimelineList.map { timeline =>
            val startYm: String = getStartYm(timeline)
            val ymMap = getTimeLineYm(timeline)
            val month = ymMap("month").toString.length match {
                case 1 => "0" + ymMap("month")
                case _ => ymMap("month")
            }
            val endYm: String = ymMap("year").toString + month
            List(startYm, endYm)
        }.reduce((lst1, lst2) => lst1 ::: lst2)
        val displayNamelst = displayNamelAndType.map(x => x._1)
        val mapDataRDD = mapData.toJavaRDD.rdd.map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString))
        val filtered_map_data = mapDataRDD.filter(x => displayNamelst.contains(x._2)).collect()
        val allDisplayName = filtered_map_data.map(x => List(x._1, x._4)).reduce((lst1, lst2) => lst1 ::: lst2).distinct
        val allDisAndType = filtered_map_data.map { x =>
            val tp = displayNamelAndType.filter(d => d._1 == x._2).head._2
            Map(x._1 -> tp, x._4 -> tp)
        }.reduce((map1, map2) => map1 ++ map2)
        val rddTemp = data.toJavaRDD.rdd.map(x => (phLycalData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
            BigDecimal(x(5).toString), BigDecimal(x(6).toString), BigDecimal(x(7).toString), x(8).toString), x(12).toString))
        val getMarket: String => String = displayname => {
            filtered_map_data.filter(x => x._1 == displayname || x._4 == displayname).head._3
        }
        val func_rmb_value: (phLycalData, String) => Boolean = (phLycalData, str) => {
            phLycalData.tp == "LC-RMB"
        }
        val func_dot_value: (phLycalData, String) => Boolean = (phLycalData, str) => {
            phLycalData.dot >= 0
        }
        val valueMap = Map("LC-RMB" -> func_rmb_value, "dot" -> func_dot_value)
        val filter_display_name = rddTemp.filter(x => x._1.date >= allTimelst.min)
            .filter(x => x._1.date <= allTimelst.max)
            .filter(x => allDisplayName.contains(x._1.display_name) && x._2 == getMarket(x._1.display_name))
            .filter(x => valueMap(allDisAndType(x._1.display_name))(x._1, x._2))
            .map { x =>
                x._1.result = if (allDisAndType(x._1.display_name) == "dot") x._1.dot
                else x._1.value
                x
            }
        val mid_sum = allTimelineList.map { timeline =>
            val startYm: String = getStartYm(timeline)
            val ymMap = getTimeLineYm(timeline)
            val month = ymMap("month").toString.length match {
                case 1 => "0" + ymMap("month")
                case _ => ymMap("month")
            }
            val endYm: String = ymMap("year").toString + month
            filter_display_name.filter(x => x._1.date >= startYm)
                .filter(x => x._1.date <= endYm)
                .keyBy(x => (x._1.display_name, timeline))
                .reduceByKey { (left, right) =>
                    left._1.result = left._1.result + right._1.result
                    left
                }
        }.reduce((rdd1, rdd2) => rdd1.union(rdd2))


        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        val mov_map_data = filtered_map_data.map(x => List((x._2, x._1), (x._2, x._4)))
            .reduce((lst1, lst2) => lst1 ::: lst2)
            .toDF("MOV_DISPLAY_NAME", "DISPLAY_NAME")
        val mid_DF = mid_sum.map(x => (x._1._1, x._1._2, x._2._1.result)).toDF("DISPLAY_NAME", "TIMELINE",
            "RESULT")
        //        val mid_DF = mid_sum.map(x => (x._1._1, x._1._2, x._2._1.result)).toDF("DISPLAY_NAME_MID", "TIMELINE",
        //            "RESULT")
        val productList = filtered_map_data.map(x => x._1).toList
        val growthResult: String => DataFrame = colString => {
            val resultDF = getGrowth(mid_DF, mov_map_data, timelineList, productList)
            resultDF
        }
        val somResult: String => DataFrame = colString => {
            val resultDF = getSom(mid_DF, mov_map_data, timelineList, productList)
            resultDF
        }
        val funcMap = Map("Growth(%)" -> growthResult, "som" -> somResult)
        val resultDF = funcMap(colList.head)(colList.head)
        resultDF
    }

    def getGrowth(data: DataFrame, mov_map_data: DataFrame, timelineList: List[String], productList: List[String]): DataFrame = {
        val mid_result = new growth().exec(Map("data" -> data, "timelineList" -> timelineList)).asInstanceOf[DataFrame]
            .withColumnRenamed("DISPLAY_NAME", "DISPLAY_NAME_MID")
        val resultDF = mid_result.join(mov_map_data, col("DISPLAY_NAME_MID") === col("DISPLAY_NAME"))
            .withColumn("TYPE", when(col("DISPLAY_NAME").isin(productList: _*), 1).otherwise(0))
        resultDF
    }

    def getSom(data: DataFrame, mov_map_data: DataFrame, timelineList: List[String], productList: List[String]): DataFrame = {
        val mid_result = data.join(mov_map_data, col("DISPLAY_NAME_MID") === col("DISPLAY_NAME"))
            .withColumn("TYPE", when(col("DISPLAY_NAME").isin(productList: _*), 1).otherwise(0))
        val mktResult = mid_result.filter(col("TYPE") === 0)
            .select("MOV_DISPLAY_NAME", "DISPLAY_NAME", "RESULT")
            .withColumnRenamed("MOV_DISPLAY_NAME", "MKT_MOV_DISPLAY_NAME")
            .withColumnRenamed("DISPLAY_NAME", "MKT_DISPLAY_NAME")
            .withColumnRenamed("RESULT", "MKT_RESULT")
        val resultDF = mid_result.filter(col("TYPE") === 1)
            .join(mktResult, col("MKT_MOV_DISPLAY_NAME") === col("MOV_DISPLAY_NAME"))
            .withColumn("SOM", (col("RESULT") / col("MKT_RESULT")) * 100)
        resultDF
    }
}