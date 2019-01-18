package com.pharbers.process.common.DTO

case class tableColArgs(var rowList: List[String], var colList: List[String], var timelineList: List[String], var displayNameList: List[String],
                        var mktDisplayName: String, var primaryValueName: String, var data: Any, var mapping: Any = null)
