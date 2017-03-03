package ml.dolphin.kaggle.outbrain

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
  * Created by Abhijit Bose (boseae@gmail.com) on 12/23/16.
  */
object FsHadoopUtils {

  def write(uri: String, filePath: String, data: Array[Byte]) = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(new Configuration())
    val path = new Path(filePath)
    val os = fs.create(path, true)
    os.write(data)
  }
}
