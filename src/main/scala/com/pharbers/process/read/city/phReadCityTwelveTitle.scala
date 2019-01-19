package com.pharbers.process.read.city

import com.pharbers.process.common.phCommand
import com.pharbers.process.read.phReadData

trait phReadCityTwelveTitle extends phReadData {
    override val start = 12
    override val step = 20
    override val cat = 2
    override val primary = 7 :: 11 :: Nil
}

class phReadCityTwelveTitleImpl extends phReadCityTwelveTitle with phCommand {
    override def exec(args : Any) : Any = this.mergeDF(args.asInstanceOf[String])
}
