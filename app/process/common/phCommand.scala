package process.common

trait phCommand {
    def perExec : Unit = Unit
    def exec
    def postExec : Unit = Unit
}
