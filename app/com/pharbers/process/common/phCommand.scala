package com.pharbers.process.common

trait phCommand {
    def perExec(args : Any) : Unit = Unit
    def exec
    def postExec : Unit = Unit
}
