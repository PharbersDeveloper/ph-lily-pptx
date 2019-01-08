package com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol

import com.pharbers.process.common.{phCommand, phLyFactory, phLyGrowthData}
import com.pharbers.process.stm.step.pptx.slider.content.phReportTableCol
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame

class movGrowth extends phCommand with phReportTableCol {
    override def exec(args: Any): Any = {
        val argsMap = args.asInstanceOf[Map[String, Any]]
        val data = argsMap("data").asInstanceOf[DataFrame]
        val timelineList = argsMap("timelineList").asInstanceOf[List[String]]
        val tmpRDD = data.toJavaRDD.rdd.map(x => phLyGrowthData(x(0).toString, x(1).toString, x(2).toString.toDouble,
            0.toString.toDouble))
        val resultRDD = timelineList.map { timeline =>
            val allTimelineList = getAllTimeline(List(timeline))
            tmpRDD.filter(x => allTimelineList.contains(x.timeline))
                .keyBy(x => x.display_name)
                .reduceByKey { (left, right) =>
                    if (left.timeline == timeline) {
                        left.growth = (left.result - right.result) / right.result * 100
                        left
                    } else {
                        right.growth = (right.result - left.result) / left.result * 100
                        right
                    }
                }
        }.reduce((rdd1, rdd2) => rdd1.union(rdd2))
        lazy val sparkDriver: phSparkDriver = phLyFactory.getCalcInstance()
        import sparkDriver.ss.implicits._
        val resultDF = resultRDD.map(iter =>
            (iter._2.display_name, iter._2.timeline, iter._2.result, iter._2.growth)
        ).toDF("DISPLAY_NAME", "TIMELINE", "RESULT", "GROWTH")
        resultDF
    }
}
