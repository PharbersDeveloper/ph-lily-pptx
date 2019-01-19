package com.pharbers.process.read.city

import com.pharbers.process.common.phCommand
import com.pharbers.process.read.phReadData

trait phReadCityEightTitle extends phReadData {
    override val start = 8
    override val step = 20
    override val cat = 2
    override val primary = 5 :: 7 :: Nil
}

class phReadCityEightTitleImpl extends phReadCityEightTitle with phCommand {
    override def exec(args : Any) : Any = this.mergeDF(args.asInstanceOf[String])
}
