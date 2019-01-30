package com.pharbers.process.stm.step.pptx.slider.content.city

import com.pharbers.process.common.{phCommand, phLycalData}
import org.apache.spark.rdd.RDD

class phCityInsulinStacked extends phCommand with phCityRankValue {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val cityList = argsMap("cityList").asInstanceOf[List[String]]
        val displayNameList = argsMap("allDisplayNames").asInstanceOf[List[String]]
        val totalChpa = argsMap("Total CHPA").asInstanceOf[String]
        val mid_sum_Map = getAllDF(args)
        val country_mid_sum = mid_sum_Map("country_mid_sum").asInstanceOf[RDD[((String, String), phLycalData)]]
        val city_mid_sum = mid_sum_Map("city_mid_sum").asInstanceOf[RDD[((String, String, String), phLycalData)]]
        val displayNameSize = displayNameList.size
        val cityResult = city_mid_sum.filter(x => cityList.contains(x._1._1))
            .map{ x =>
                val displayNameIndex = displayNameList.indexOf(x._1._3)
                ((x._1._2, List.fill(displayNameIndex)(BigDecimal(0)) ::: List(x._2.result) :::
                    List.fill(displayNameSize-displayNameIndex-1)(BigDecimal(0))), x._2)
            }.keyBy(x => (x._2.id, x._1._1))
            .reduceByKey{ (left, right) =>
                val valueList = left._1._2.zip(right._1._2).map(x => x._1 + x._2)
                ((left._1._1, valueList), left._2)
            }.map{x =>
                val toalValue = x._2._1._2.sum
                val somList = x._2._1._2.map(_/toalValue)
                (x._1._1, x._2._1._1, somList)
            }
        val countryResult = country_mid_sum.map{x =>
            val displayNameIndex = displayNameList.indexOf(x._1._1)
            ((x._1._2, List.fill(displayNameIndex)(BigDecimal(0)) ::: List(x._2.result) :::
                List.fill(displayNameSize-displayNameIndex-1)(BigDecimal(0))), x._2)
            }.keyBy(x => x._1._1)
            .reduceByKey{ (left, right) =>
                val valueList = left._1._2.zip(right._1._2).map(x => x._1 + x._2)
                ((left._1._1, valueList), left._2)
            }.map{ x =>
                val toalValue = x._2._1._2.sum
                val somList = x._2._1._2.map(_/toalValue)
                (totalChpa, x._1, somList)
            }
        val resultRDD = cityResult.union(countryResult)
        resultRDD
    }
}
