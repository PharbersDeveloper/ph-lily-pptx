package com.pharbers.pactions.generalactions

import com.pharbers.pactions.actionbase.{DFArgs, StringArgs, pActionArgs, pActionTrait}
import com.pharbers.spark.phSparkDriver

object readCsvNoTitleAction{
    def apply(arg_path: String, delimiter: String = ",",
              arg_name: String = "readCsvNoTitleJob", applicationName: String = "test-driver"): pActionTrait =
        new readCsvNoTitleAction(StringArgs(arg_path), delimiter, arg_name, applicationName)
}

class readCsvNoTitleAction(override val defaultArgs: pActionArgs, delimiter: String,
                           override val name: String, applicationName: String) extends pActionTrait{
    override def perform(args: pActionArgs): pActionArgs =
        DFArgs(phSparkDriver(applicationName).readCsvNoTitle(defaultArgs.asInstanceOf[StringArgs].get, delimiter))
}
