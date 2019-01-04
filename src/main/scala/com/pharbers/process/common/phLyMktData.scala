package com.pharbers.process.common

case class phLyMktData(
                             manuf_type: String,
                             date: String,
                             value: BigDecimal
                         ) {
    override def toString: String = s"$manuf_type, $date, $value"
}
