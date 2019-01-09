package com.pharbers.process.common.jsonData

case class phTable(var factory: String, var mkt_display: String, var mkt_col: String, var pos: List[Int], var timeline: List[String], var col: col,  var row: row)

case class col(var title: String, var count: List[String])

case class row(var title: String, var display_name: List[String])