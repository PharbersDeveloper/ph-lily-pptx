package com.pharbers.process.common

import com.pharbers.spark.phSparkDriver

import scala.reflect.runtime.{universe => ru}
import org.apache.spark.sql.DataFrame

object phLyFactory {
    var stssoo : Map[String, Any] = Map.empty
    lazy val sparkDriver: phSparkDriver = phSparkDriver("cui-test")
    import sparkDriver.ss.implicits._

    def getInstance(name : String) : Any = {
        println(s"create instance for $name")
        val m = ru.runtimeMirror(getClass.getClassLoader)
        val clssyb = m.classSymbol(Class.forName(name))
        val cm = m.reflectClass(clssyb)
        val ctor = clssyb.toType.decl(ru.termNames.CONSTRUCTOR).asMethod
        val ctorm = cm.reflectConstructor(ctor)
        val tmp = ctorm()
        tmp
    }

    def getStorageWithName(name : String) : DataFrame = {
        stssoo.getOrElse(name, throw new Exception("error key")).asInstanceOf[DataFrame]
    }

    def getCalcInstance() : phSparkDriver = sparkDriver
}
