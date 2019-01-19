package com.pharbers.process.merge

import com.pharbers.process.common.{phCommand, phLyCityDataSet, phLyFactory}
import org.apache.spark.rdd.RDD

class phMergeCityDistinct {

}

class phMergeCityDistinctImpl extends phMergeCityDistinct with phCommand {
    override def exec(args: Any): Any = {
        /**
          * 1. storage里面取所有读取的数据
          */
        val rdd_name_lst = args.asInstanceOf[Option[List[String]]].get

        val rdd_lst = rdd_name_lst.map { name =>
            phLyFactory.stssoo(name).asInstanceOf[RDD[phLyCityDataSet]]
        }

        /**
          * 2. 将数据文件union起来
          */
        def unionAcc(lst: List[RDD[phLyCityDataSet]], reval: Option[RDD[phLyCityDataSet]]) : Option[RDD[phLyCityDataSet]] = {
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
        val rdd_rmb = result.filter(x => x.tp == "LC-RMB")
        val rdd_rmb_new = rdd_rmb.keyBy(row => row.product_name + row.pack_des + row.date + row.city).reduceByKey{ (left, right) =>
            assert(left.value == right.value)
            left
        }.map (x => x._2)
        val rdd_not_rmb = result.filter(x => x.tp != "LC-RMB").keyBy(row => row.product_name + row.pack_des + row.date + row.city).reduceByKey { (left, right) =>
            assert(left.value == right.value)
            left
        }.map (x => x._2)
        //        assert(rdd_rmb.count() + rdd_not_rmb.count() == result.count())

        val reval = rdd_rmb_new.union(rdd_not_rmb)
        phLyFactory.clearStorage
        phLyFactory.stssoo = phLyFactory.stssoo + ("city main frame" -> reval)
        phLyFactory.saveMidProcess("city main frame", "hdfs:///test/mid/main-frame-without-dot/")
        Some(reval)
    }
}