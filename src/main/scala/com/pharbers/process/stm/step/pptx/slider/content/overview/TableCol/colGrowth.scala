package com.pharbers.process.stm.step.pptx.slider.content.overview.TableCol

import com.pharbers.process.common.{phLyMOVData, phLycalArray}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class colGrowth {
    def col(data: DataFrame, displayNamelList: List[String], allTimelst: List[String], forward:Int, mktDisplayName: String, timelineList: List[String])
           (fun:  RDD[(String, List[BigDecimal])] => RDD[(String, List[String])]): RDD[(String, List[String])] ={
        val rddTemp = data.toJavaRDD.rdd.map(x =>  phLyMOVData(x(0).toString, x(1).toString, x(2).toString, BigDecimal(x(3).toString)))


        val filter_display_name = rddTemp.filter(x => displayNamelList.contains(x.id))


        val mid_sum = filter_display_name.map { x =>
            val idx = allTimelst.indexOf(x.date)
            val lst = if (idx > -1) {
                List.fill(idx)(BigDecimal(0)) :::
                        List.fill(forward)(x.value) :::
                        List.fill(allTimelst.length - idx - forward)(BigDecimal(0))
            } else List.fill(allTimelst.length)(BigDecimal(0))
            (x, phLycalArray(lst))
        }.keyBy(_._1.id)
                .reduceByKey { (left, right) =>
                    val lst = left._2.reVal.zip(right._2.reVal).map(x => x._1 + x._2)
                    (left._1, phLycalArray(lst))
                }.map(x => (x._1, x._2._2.reVal.reverse))

        fun(mid_sum)
    }
}

object colGrowth {
//    val func_growth: RDD[(String, List[BigDecimal])] => RDD[(String, List[String])] = mid_sum => {
//        mid_sum.map { iter =>
//            val growth: List[String] = iter._2.zipWithIndex.map { case (value, idx) =>
//                if (idx >= timelineList.length) {
//                    BigDecimal(20181231).toString()
//                } else {
//                    val m = iter._2.apply(idx + 12)
//                    if (m == 0) "Nan"
//                    else (((value - m) / m) * 100).toString()
//                }
//            }
//            (iter._1, growth.take(timelineList.length).reverse)
//        }
//    }
    val func_som: Int => RDD[(String, List[BigDecimal])] => RDD[(String, List[String])] = length => mid_sum => {
        val mktDisplayNameList = mid_sum.reduce((left, right) => {
            val lst = left._2.zip(right._2).map(x => x._1 + x._2)
            ("all", lst)
        })._2
        mid_sum.map { iter =>
            val som = iter._2.zipWithIndex.map { case (value, idx) =>
                if (idx >= length) {
                    BigDecimal(20181231).toString()
                } else {
                    val m = mktDisplayNameList(idx)
                    if (m == 0) "NaN"
                    else ((value / mktDisplayNameList(idx)) * 100).toString()
                }
            }
            (iter._1, som.take(length).reverse)
        }
    }
}