package com.pharbers.process.read

import com.pharbers.process.common.phCommand

trait phReadDataFullTitle extends phReadData {
    override val start = 14
    override val step = 60
    override val cat = 2
    override val primary = 9 :: 13 :: Nil
}

class phReadDataFullTitleImpl extends phReadDataFullTitle with phCommand {
    override def exec(args: Any): Any = this.mergeDF(args.asInstanceOf[String])
}