package com.pharbers.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

trait spark_conn_instance {
    //    System.setProperty("HADOOP_USER_NAME","spark")
    val applicationName: String = ""
    private val conf = new SparkConf()
        .set("spark.yarn.jars", "hdfs://spark.master:9000/jars/sparkJars")
        .set("spark.yarn.archive", "hdfs://spark.master:9000/jars/sparkJars")
        .set("yarn.resourcemanager.address", "spark.master:8032")
        .setAppName(applicationName)
        .setMaster("yarn")
        .set("spark.sql.crossJoin.enabled", "true")
        .set("yarn.resourcemanager.hostname", "spark.master")
        .set("spark.yarn.dist.files", "hdfs://spark.master:9000/config")
        .set("spark.executor.memory", "2g")
        .set("spark.driver.extraJavaOptions", "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,adress=5005")
        .set("spark.executor.extraJavaOptions",
            """
              | -XX:+UseG1GC -XX:+PrintFlagsFinal
              | -XX:+PrintReferenceGC -verbose:gc
              | -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
              | -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions
              | -XX:+G1SummarizeConcMark
              | -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=1
            """.stripMargin)

    val spark_session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val spark_context: SparkContext = spark_session.sparkContext
    val spark_sql_context: SQLContext = spark_session.sqlContext
}
