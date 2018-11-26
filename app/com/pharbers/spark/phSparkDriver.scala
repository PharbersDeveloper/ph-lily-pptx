package com.pharbers.spark

case class phSparkDriver(override val applicationName: String = "test-dirver") extends spark_conn_trait with spark_managers
