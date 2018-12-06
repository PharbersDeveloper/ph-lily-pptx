package com.pharbers.process.common

import java.util.UUID

import com.pharbers.spark.phSparkDriver
import org.apache.spark.rdd.RDD

import scala.reflect.runtime.{universe => ru}
import org.apache.spark.sql.DataFrame

object phLyFactory {
    var stssoo : Map[String, Any] = Map.empty
    lazy val sparkDriver: phSparkDriver = phSparkDriver("cui-test")
    import sparkDriver.ss.implicits._

    var isSaveMidDoc = false
    var uuid : String = ""

    uuid = UUID.randomUUID().toString
    def startProcess: String = uuid
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
    def getStorageWithDFName(name: String): DataFrame = stssoo.get(name).get.asInstanceOf[DataFrame]
    def clearStorage = stssoo = Map.empty

    def getCalcInstance() : phSparkDriver = sparkDriver
    def phRow2DF(name : String) : DataFrame = getStorageWithName(name).toDF()
    def phRow2DFDetail(name : String) : DataFrame = getStorageWithName(name).map { iter =>
            (iter._1, iter._2.product_name, iter._2.pack_des, iter._2.date, iter._2.tp, iter._2.add_rate, iter._2.dot, iter._2.value)
        }.toDF("ID", "PRODUCT NAME", "PACK DES", "DATE", "TYPE", "ADD RATE", "DOT", "VALUE")
}
