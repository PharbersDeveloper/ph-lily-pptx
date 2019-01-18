package com.pharbers.process.read.city

import com.pharbers.process.common.phCommand
import com.pharbers.process.read.phReadData

trait phReadCityFullTitle extends phReadData {
    override val start = 14
    override val step = 20
    override val cat = 2
    override val primary = 9 :: 13 :: Nil
}

class phReadCityFullTitleImpl extends phReadCityFullTitle with phCommand {
    override def exec(args: Any): Any = this.mergeDF(args.asInstanceOf[String])
}
