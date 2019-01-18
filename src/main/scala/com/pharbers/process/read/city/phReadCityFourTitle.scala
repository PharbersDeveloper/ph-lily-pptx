package com.pharbers.process.read.city

import com.pharbers.process.common.phCommand
import com.pharbers.process.read.phReadData

trait phReadCityFourTitle extends phReadData {
    override val start = 4
    override val step = 20
    override val cat = 2
    override val primary = 1 :: 3 :: Nil
}

class phReadCityFourTitleImpl extends phReadCityFourTitle with phCommand {
    override def exec(args : Any) : Any = this.mergeDF(args.asInstanceOf[String])
}