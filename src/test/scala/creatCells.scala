object creatCells extends App {
    val cellNumber: (Int, Int) = (17, 10)
    List.fill(cellNumber._1)(List.fill(cellNumber._2)("chc_table_white")).zipWithIndex.foreach(x => {
        x._1.zipWithIndex.foreach(y => {
            val column = ('A'.toInt + x._2).toChar.toString
            val row = y._2 + 1
            val css = y._1
            val t = if(x._2 == 0 || y._2 == 0) "String" else "Number"
            println("\"" + s"#c#$column$row#s#$css#t#$t#v#" + "\",")
        })
    })
}
