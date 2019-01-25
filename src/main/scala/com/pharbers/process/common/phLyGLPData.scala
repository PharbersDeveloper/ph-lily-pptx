package com.pharbers.process.common

case class phLyGLPData(
                     TC_II_SHORT: String,
                     TC_IV_SHORT: String,
                     date: String,
                     tp: String,
                     var value: BigDecimal,
                     var add_rate: BigDecimal = 1,
                     var dot: BigDecimal = -1,
                     var city: String = "default"
                 ) {
    override def toString: String = s"$TC_II_SHORT, $TC_IV_SHORT, $date, $tp, $value, $add_rate, $dot, $city"
}
