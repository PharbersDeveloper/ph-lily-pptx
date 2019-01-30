package com.pharbers.process.stm.step.pptx.slider.content.city

import com.pharbers.process.common.{phCommand, phLyFactory, phLycalData}
import com.pharbers.spark.phSparkDriver
import org.apache.spark.rdd.RDD

class phCityRank extends phCommand with phCityRankValue {
    override def exec(args: Any): Any = {
        val mktDisplayName = args.asInstanceOf[Map[String, Any]]("mktDisplayName").asInstanceOf[String]
        val mid_sum_Map = getAllDF(args)
        val country_mid_sum = mid_sum_Map("country_mid_sum").asInstanceOf[RDD[((String, String), phLycalData)]]
        val city_mid_sum = mid_sum_Map("city_mid_sum").asInstanceOf[RDD[((String, String, String), phLycalData)]]
        val mktDisplayNameSum = country_mid_sum.filter(x => x._1._1 == mktDisplayName)
            .map(x => x._2.result).sum()
        val resultList = city_mid_sum.filter(x => x._1._3 == mktDisplayName)
            .map(x => (x._2.id, x._1._2, (x._2.result / mktDisplayNameSum) *100))
            .sortBy(x => -x._3)
        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        val resultDF = resultList.toDF("CITY", "TIMELINE", "SOM in " + mktDisplayName)
        resultDF
    }
}
