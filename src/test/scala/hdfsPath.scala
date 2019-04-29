import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

object hdfsPath extends App {
    val path = "/test"
    val conf = new Configuration
    val hdfs = FileSystem.get(conf)
    val hdfsPath = new Path(path)
//    FileUtil.copyMerge(hdfs, new Path("/workData/Panel/001325b0-413d-42c6-9792-314153e168c7"), hdfs, new Path("/test/dcs/testMerge"),false,conf,null)
    hdfs.copyToLocalFile(false,new Path("/workData/Export/1393506e-aedc-952b-6c96-f8eb6d349ba1"), new Path("D:\\文件\\test"),true)
    val filePathlst = hdfs.listStatus(hdfsPath)
    filePathlst.filter(x => x.isDirectory)
        .foreach{x =>
        println(x.getPath.getName)
    }
}
