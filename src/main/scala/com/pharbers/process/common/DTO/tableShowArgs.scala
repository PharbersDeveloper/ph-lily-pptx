package com.pharbers.process.common.DTO

case class tableShowArgs(var rowList: List[(String, String)], colList: List[(String, String)], timelineList: List[(String, String)], mktDisplayName: String,
                         pos: List[Int], colTitle: (String, String), rowTitle: (String, String), slideIndex: Int, col2DataColMap: Map[String, String])

