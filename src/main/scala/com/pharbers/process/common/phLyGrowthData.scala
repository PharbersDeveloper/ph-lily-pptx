package com.pharbers.process.common

case class phLyGrowthData(
                             display_name: String,
                             timeline: String,
                             result: Double,
                             var growth: Double = 0
                         ) {
    override def toString: String =
        s"$display_name, $timeline, $result, $growth"
}
