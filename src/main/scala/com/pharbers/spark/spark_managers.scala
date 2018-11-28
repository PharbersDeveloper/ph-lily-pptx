package com.pharbers.spark

import org.apache.spark.sql.DataFrame

trait spark_managers extends csv2RDD with readCsv with readCsvNoTitle

sealed trait spark_manager_trait {
    implicit val conn_instance: spark_conn_instance
    val ss = conn_instance.spark_session
    val sc = conn_instance.spark_context
    val sqc = conn_instance.spark_sql_context
}

trait csv2RDD extends spark_manager_trait {
    def csv2RDD(file_path: String,
                delimiter: String = ",", header: Boolean = true) = {
        ss.read.format("csv")
            .option("header", header)
            .option("inferSchema", true.toString)
            .option("mode", "DROPMALFORMED")
            .option("delimiter", delimiter)
            .csv(file_path)
    }
}

trait readCsv extends spark_manager_trait {
    def readCsv(file_path: String, delimiter: String = ",") = {
        ss.read.format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", delimiter)
            .load(file_path)
    }
}

trait readCsvNoTitle extends spark_manager_trait {
    def readCsvNoTitle(filePath: String, delimiter: String = ","): DataFrame = {
        ss.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", ",")
            .load(filePath)
    }
}
