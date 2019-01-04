package com.pharbers.process.read.mktoverview

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame

trait calcMapTable extends java.io.Serializable {
    val sparkDriver = phLyFactory.getCalcInstance()
    import org.apache.spark.sql.functions.col
    def getDF(path: String, resultName: String): Unit ={
        val df: DataFrame = sparkDriver.ss.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", ",")
            .load(path)
        val resultDF = df.filter(col("_c0") =!= "Display Name")
            .withColumnRenamed("_c0", "DISPLAYNAME")
            .withColumnRenamed("_c1", "ID")
        phLyFactory.setStorageWithDFName(resultName, resultDF)
    }
}

class phReadMktOne extends calcMapTable with phCommand{
    override def exec(args: Any): Any = this.getDF(args.asInstanceOf[String], "movMktOne")
}

class phReadMktTwo extends calcMapTable with phCommand{
    override def exec(args: Any): Any = this.getDF(args.asInstanceOf[String], "movMktTwo")
}

class phReadMktThree extends calcMapTable with phCommand{
    override def exec(args: Any): Any = {
        import org.apache.spark.sql.functions.col
        val path: String = args.asInstanceOf[String]
        val df: DataFrame = sparkDriver.ss.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", ",")
            .load(path)
        val resultDF = df.filter(col("_c0") =!= "CORPORATE DESC")
            .withColumnRenamed("_c0", "ID")
        phLyFactory.setStorageWithDFName("movMktThree", resultDF)
    }
}
