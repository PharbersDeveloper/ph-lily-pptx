package com.pharbers.process.merge

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame

class phMergeSearch extends phCommand{
    override def exec(args: Any): DataFrame = {
        val sourceDF = phLyFactory.getStorageWithName("gen_data_set")
//        val sourceDF = phLyFactory.getCalcInstance().readCsv("/test/result03")
        val displayDF = args.asInstanceOf[List[DataFrame]].reduce(_ union _)
        val fullDF: DataFrame = sourceDF.join(displayDF, sourceDF("PRODUCT DESC") === displayDF("PRODUCT_DESC_MARKET")
                && sourceDF("PACK DESC") === displayDF("PACK_DESC_MARKET"))
        print("phMergeSearch over")
        fullDF
    }
}
