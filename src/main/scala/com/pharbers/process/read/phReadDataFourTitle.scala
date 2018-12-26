package com.pharbers.process.read

import com.pharbers.process.common.{phCommand, phLyFactory}

trait phReadDataFourTitle extends phReadData {
    override val start = 4
    override val step = 60
    override val cat = 2
    override val primary = 1 :: 3 :: Nil
}

class phReadDataFourTitleImpl extends phReadDataFourTitle with phCommand {
    override def exec(args : Any) : Any = this.mergeDF(args.asInstanceOf[String])
}