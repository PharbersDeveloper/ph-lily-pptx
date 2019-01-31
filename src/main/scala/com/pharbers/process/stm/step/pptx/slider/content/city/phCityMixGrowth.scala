package com.pharbers.process.stm.step.pptx.slider.content.city

import com.pharbers.process.common.{phCommand, phLycalData}
import org.apache.spark.rdd.RDD

class phCityMixGrowth extends phCommand with phCityRankValue {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val mktDisplayName = argsMap("mktDisplayName").asInstanceOf[String]
        val timelineList = argsMap("timelineList").asInstanceOf[List[String]]
        val cityList = argsMap("cityList").asInstanceOf[List[String]]
        val displayNameList = argsMap("allDisplayNames").asInstanceOf[List[String]]
        val totalChpa = argsMap("Total CHPA").asInstanceOf[String]
        val displayNameSize = displayNameList.size
        val mktDisplayNameIndex = displayNameList.indexOf(mktDisplayName)
        val proDisplayName = displayNameList.filter(x => x != mktDisplayName).head
        val proDisplayNameIndex = displayNameList.indexOf(proDisplayName)
        val func_Timeline: List[String] => List[String] = lst => lst.map { timeline =>
            List(timeline, timeline.dropRight(5) + timeline.takeRight(5).take(3) + (timeline.takeRight(2).toInt - 1).toString)
        }.reduce((lst1, lst2) => (lst1 ::: lst2).distinct).sorted
        val timelineListTemp = func_Timeline(timelineList)
        val allTimelineList = func_Timeline(timelineListTemp)
        val alltimelineSize = allTimelineList.size
        val mid_sum_Map = getAllDF(argsMap ++ Map("timelineList" -> timelineListTemp) ++ Map("colList" -> List("Growth(%)")))
        val country_mid_sum = mid_sum_Map("country_mid_sum").asInstanceOf[RDD[((String, String), phLycalData)]]
        val city_mid_sum = mid_sum_Map("city_mid_sum").asInstanceOf[RDD[((String, String, String), phLycalData)]]
        val cityResultRDD = city_mid_sum.filter(x => cityList.contains(x._1._1)).map { x =>
            val displayNameIndex = displayNameList.indexOf(x._1._3)
            val timelineIndex = allTimelineList.indexOf(x._1._2)
            val valueList = List.fill(displayNameIndex)(BigDecimal(0)) ::: List(x._2.result) ::: List.fill(displayNameSize - displayNameIndex - 1)(BigDecimal(0))
            (x, List.fill(timelineIndex)(List.fill(displayNameSize)(BigDecimal(0))) ::: List(valueList) :::
                    List.fill(alltimelineSize - timelineIndex - 1)(List.fill(displayNameSize)(BigDecimal(0))))
        }.keyBy(x => x._1._1._1).reduceByKey { (left, right) =>
            val valueList = left._2.zip(right._2).map { x =>
                x._1.zip(x._2).map(x => x._1 + x._2)
            }
            (left._1, valueList)
        }.map { x =>
            val valueList = x._2._2
            val somIncrease = (valueList(2)(proDisplayNameIndex) / valueList(2)(mktDisplayNameIndex) - valueList(1)(proDisplayNameIndex) / valueList(1)(mktDisplayNameIndex)) * 100
            val mktGrowth = (valueList(2)(mktDisplayNameIndex) - valueList(1)(mktDisplayNameIndex)) * 100 / valueList(1)(mktDisplayNameIndex)
            val growthIncrease = (valueList(2)(proDisplayNameIndex) - valueList(1)(proDisplayNameIndex)) * 100 / valueList(1)(proDisplayNameIndex) - mktGrowth
                    (x._1, List(somIncrease, growthIncrease, mktGrowth))
        }
        val countryResultRDD = country_mid_sum.map { x =>
            val displayNameIndex = displayNameList.indexOf(x._1._1)
            val timelineIndex = allTimelineList.indexOf(x._1._2)
            val valueList = List.fill(displayNameIndex)(BigDecimal(0)) ::: List(x._2.result) ::: List.fill(displayNameSize - displayNameIndex - 1)(BigDecimal(0))
            (x, List.fill(timelineIndex)(List.fill(displayNameSize)(BigDecimal(0))) ::: List(valueList) :::
                    List.fill(alltimelineSize - timelineIndex - 1)(List.fill(displayNameSize)(BigDecimal(0))))
        }.keyBy(x => totalChpa).reduceByKey { (left, right) =>
            val valueList = left._2.zip(right._2).map { x =>
                x._1.zip(x._2).map(x => x._1 + x._2)
            }
            (left._1, valueList)
        }.map { x =>
            val valueList = x._2._2
            val somIncrease = (valueList(2)(proDisplayNameIndex) / valueList(2)(mktDisplayNameIndex) - valueList(1)(proDisplayNameIndex) / valueList(1)(mktDisplayNameIndex)) * 100
            val mktGrowth = (valueList(2)(mktDisplayNameIndex) - valueList(1)(mktDisplayNameIndex)) * 100 / valueList(1)(mktDisplayNameIndex)
            val growthIncrease = (valueList(2)(proDisplayNameIndex) - valueList(1)(proDisplayNameIndex)) * 100 / valueList(1)(proDisplayNameIndex) - mktGrowth
            (x._1, List(somIncrease, growthIncrease, mktGrowth))
        }
        val resultRDD = countryResultRDD.union(cityResultRDD)
        resultRDD
    }
}
