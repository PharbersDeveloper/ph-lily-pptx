package com.pharbers.process.merge

import java.util.UUID

import com.pharbers.process.common.{phCommand, phLyDataSet, phLyFactory}
import org.apache.spark.rdd.RDD

trait phMergeDistinct {

}

class phMergeDistinctImpl extends phMergeDistinct with phCommand {
    override def exec(args: Any): Any = {
        /**
          * 1. storage里面取所有读取的数据
          */
        val rdd_name_lst = args.asInstanceOf[Option[List[String]]].get
        rdd_name_lst.foreach(println)

        val rdd_lst = rdd_name_lst.map { name =>
            phLyFactory.getStorageWithName(name)
        }

        rdd_lst.foreach(println)

        /**
          * 2. 将数据文件union起来
          */
        def unionAcc(lst: List[RDD[(String, phLyDataSet)]], reval: Option[RDD[(String, phLyDataSet)]]) : Option[RDD[(String, phLyDataSet)]] = {
            if (lst.isEmpty) reval
            else {
                val cur = lst.head
                if (reval.isEmpty) unionAcc(lst.tail, Some(cur))
                else unionAcc(lst.tail, Some(reval.get.union(cur)))
            }
        }
        val result = unionAcc(rdd_lst, None).get

        /**
          * 3. 去重复
          */
        val rdd_rmb = result.filter(x => x._2.tp == "LC-RMB")
        val rdd_rmb_new = rdd_rmb.keyBy(row => row._1 + "===" + row._2.date).reduceByKey{ (left, right) =>
            assert(left._2.value == right._2.value)
            left
        }.map (x => x._2)
        val rdd_not_rmb = result.filter(x => x._2.tp != "LC-RMB").keyBy(row => row._1 + "===" + row._2.date).reduceByKey { (left, right) =>
            assert(left._2.value == right._2.value)
            left
        }.map (x => x._2)
//        assert(rdd_rmb.count() + rdd_not_rmb.count() == result.count())

        val reval = rdd_rmb_new.union(rdd_not_rmb)
        phLyFactory.clearStorage
        phLyFactory.setStorageWithName("main frame", reval)
        phLyFactory.saveMidProcess("main frame", "hdfs:///test/mid/main-frame-without-dot/")
        println(reval.count())
        Some(reval)
    }
}
