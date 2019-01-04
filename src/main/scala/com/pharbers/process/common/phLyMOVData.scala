package com.pharbers.process.common

case class phLyMOVData(
                          id: String,
                          date: String,
                          tp: String,
                          value: BigDecimal
                          ) {
    override def toString: String = s"$id, $date, $tp, $value"
}
