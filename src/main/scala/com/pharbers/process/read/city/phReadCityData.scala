package com.pharbers.process.read.city

import java.util.Date

import com.pharbers.process.common.{phCommand, phLyCityDataSet, phLyDataSet, phLyFactory}
import com.pharbers.process.read._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD

class phReadCityData extends phCommand{
    override def exec(args: Any): Any = {
        val path = args.asInstanceOf[String]
        val city = path.split("/").last
        val conf = new Configuration
        val hdfs = FileSystem.get(conf)
        val hdfsPath = new Path(path)
        val filePathlst = hdfs.listStatus(hdfsPath).filter(x => x.isDirectory)
            .map(x => x.getPath)
        val funcFullTitle: String => String = path => {
            new phReadCityFullTitleImpl().exec(path).asInstanceOf[String]
        }
        val funcTwelveTitle: String => String = path => {
            new phReadCityTwelveTitleImpl().exec(path).asInstanceOf[String]
        }
        val funcEightTitle: String => String = path => {
            new phReadCityEightTitleImpl().exec(path).asInstanceOf[String]
        }
        val funcSixTitle: String => String = path => {
            new phReadCitySixTitleImpl().exec(path).asInstanceOf[String]
        }
        val funcFourTitle: String => String = path => {
            new phReadCityFourTitleImpl().exec(path).asInstanceOf[String]
        }
        val funcMap = Map("fullTitle" -> funcFullTitle, "twelveTitle" -> funcTwelveTitle, "eightTitle" -> funcEightTitle,
        "sixTitle" -> funcSixTitle, "fourTitle" -> funcFourTitle)
        val rdd_name_lst = filePathlst.map(x => funcMap(x.getName)(x.toString))
        val rdd_lst = rdd_name_lst.map { name =>
            phLyFactory.getStorageWithName(name)
        }.toList
        val unionedRDD = unionAcc(rdd_lst, None).get
        val rddResult = unionedRDD.map(x => phLyCityDataSet(city, x._2.product_name, x._2.pack_des, x._2.date, x._2.tp,
            x._2.value, x._2.add_rate, x._2.dot))
        val result = md5Hash(s"$city#merge" + new Date().getTime)
        phLyFactory.stssoo = phLyFactory.stssoo + (result -> rddResult)
        result
    }
    def md5Hash(text : String):String = java.security.MessageDigest.
        getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map{"%02x".format(_)}.foldLeft(""){_+_}
    def unionAcc(lst: List[RDD[(String, phLyDataSet)]], reval: Option[RDD[(String, phLyDataSet)]]) : Option[RDD[(String, phLyDataSet)]] = {
        if (lst.isEmpty) reval
        else {
            val cur = lst.head
            if (reval.isEmpty) unionAcc(lst.tail, Some(cur))
            else unionAcc(lst.tail, Some(reval.get.union(cur)))
        }
    }
}
