package com.pharbers.process.common

import java.util.UUID

import com.pharbers.spark.phSparkDriver
import org.apache.spark.rdd.RDD

import scala.reflect.runtime.{universe => ru}
import org.apache.spark.sql.DataFrame

object phLyFactory {
    var stssoo : Map[String, AnyRef] = Map.empty
    lazy val sparkDriver: phSparkDriver = phSparkDriver("cui-test")
    import sparkDriver.ss.implicits._

    var isSaveMidDoc = false
    var uuid : String = ""

    def startProcess = uuid = UUID.randomUUID().toString
    def endProcess = uuid = ""
    def setSaveMidDoc = isSaveMidDoc = true
    def saveMidProcess(name : String, path : String) = {
        if (isSaveMidDoc) {
            val result = getStorageWithName(name)
            result.coalesce(1).saveAsTextFile(path + phLyFactory.uuid)
        }
    }

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

    def setStorageWithName(name : String, data : RDD[(String, phLyDataSet)]) = stssoo = stssoo + (name -> data)
    def getStorageWithName(name : String) : RDD[(String, phLyDataSet)] =
        stssoo.get(name).map (_.asInstanceOf[RDD[(String, phLyDataSet)]]).
            getOrElse(throw new Exception("RDD 不存在"))
    def clearStorage = stssoo = Map.empty

    def getCalcInstance() : phSparkDriver = sparkDriver
    def phRow2DF(name : String) : DataFrame = getStorageWithName(name).toDF()
}
