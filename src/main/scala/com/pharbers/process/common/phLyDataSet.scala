package com.pharbers.process.common

case class phLyDataSet(
                      product_name: String,
                      pack_des: String,
                      date: String,
                      tp: String,
                      var value: BigDecimal,
                      var add_rate: BigDecimal = 1,
                      var dot: BigDecimal = -1
                      ) {
    override def toString: String = s"$product_name, $pack_des, $date, $tp, $value, $add_rate, $dot"
}
