package com.pharbers.process.common

case class phLyProdData(
                          product: String,
                          date: String,
                          tp: String,
                          value: BigDecimal
                          ) {
    override def toString: String = s"$product, $tp, $value"
}
