package com.pharbers.spark

trait spark_conn_trait {
    val applicationName: String = ""
    implicit val conn_instance = spark_conn_obj(applicationName)
}
