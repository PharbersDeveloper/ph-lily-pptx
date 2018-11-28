package com.pharbers.process.common

trait phCommand {
    def preExec(args : Any) : Unit = Unit
    def exec(args : Any) : Any
}
