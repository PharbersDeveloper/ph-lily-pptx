package com.pharbers.process.common.DTO

case class tableCells(var readyCells: List[String], var noValueCells: Map[(String, String, String), cell]){
    def allReady(): Unit ={
        val cells =noValueCells.values.map(x => {
            val cellIndex = x.cell
            val css = x.cssName.mkString("*")
            val cate = x.cate
            val value = x.value
            s"#c#$cellIndex#s#$css#t#$cate#v#$value"
        })
        readyCells = readyCells ::: cells.toList
    }
}

case class cell(cell: String, var value: String, cate: String, cssName: List[String], data2Vale: String => String = s => s){
    def setValue(data: String): Unit ={
        value = data2Vale(data)
    }
}
