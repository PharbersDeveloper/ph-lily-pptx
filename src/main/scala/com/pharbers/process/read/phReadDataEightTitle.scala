package com.pharbers.process.read

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

trait phReadDataEightTitle extends phReadData {
    override val start = 8
    override val step = 60
    override val cat = 2
    override val primary = 5 :: 7 :: Nil
}

class phReadDataEightTitleImpl extends phReadDataEightTitle with phCommand {
    override def exec(args : Any) : Any = this.mergeDF(args.asInstanceOf[String])
}