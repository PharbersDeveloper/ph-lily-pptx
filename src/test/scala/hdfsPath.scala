import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object hdfsPath extends App {
    val path = "/test"
    val conf = new Configuration
    val hdfs = FileSystem.get(conf)
    val hdfsPath = new Path(path)
    val filePathlst = hdfs.listStatus(hdfsPath)
    filePathlst.filter(x => x.isDirectory)
        .foreach{x =>
        println(x.getPath.getName)
    }
}
