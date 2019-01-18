package com.pharbers.process.read.city

import com.pharbers.process.common.phCommand
import com.pharbers.process.read.phReadData

trait phReadCitySixTitle extends phReadData {
    override val start = 6
    override val step = 20
    override val cat = 2
    override val primary = 3 :: 5 :: Nil
}

class phReadCitySixTitleImpl extends phReadCitySixTitle with phCommand {
    override def exec(args : Any) : Any = this.mergeDF(args.asInstanceOf[String])
}
