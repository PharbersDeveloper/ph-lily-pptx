package com.pharbers.process.common

case class phLyManufaData(
                                corporate: String,
                                date: String,
                                tp: String,
                                value: BigDecimal
                            ) {
    override def toString: String = s"$corporate, $date, $tp, $value"
}
