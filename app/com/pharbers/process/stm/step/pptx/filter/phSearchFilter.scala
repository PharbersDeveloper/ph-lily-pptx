package com.pharbers.process.stm.step.pptx.filter

import com.pharbers.process.common.phCommand
import org.apache.spark.sql.DataFrame

trait phSearchFilter{

}

class phSearchFilterImp extends phSearchFilter with phCommand {
    override def exec(args: Any): DataFrame = {

    }
}
