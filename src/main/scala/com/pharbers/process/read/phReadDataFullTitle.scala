package com.pharbers.process.read

import com.pharbers.process.common.{phCommand, phLyFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

trait phReadDataFullTitle extends phReadData {
    override val start = 14
    override val step = 60
    override val cat = 2
    override val primary = 9 :: 13 :: Nil
}

class phReadDataFullTitleImpl extends phReadDataFullTitle with phCommand {
    override def exec(args: Any): Any = this.formatDF(args.asInstanceOf[String])
}