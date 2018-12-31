package com.pharbers.process.common

case class phLycalData(id: String,
                       product_name: String,
                       pack_des: String,
                       date: String,
                       tp: String,
                       add_rate: BigDecimal,
                       dot: BigDecimal,
                       value: BigDecimal,
                       display_name: String,
                       var result: BigDecimal = 0
                      ) {
    override def toString: String =
        s"$product_name, $pack_des, $date, $tp, $value, $add_rate, $dot, $display_name, $result"
}
