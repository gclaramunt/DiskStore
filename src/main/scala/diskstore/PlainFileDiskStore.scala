package diskstore

import java.io.{FileOutputStream, File}


/**
 * User: Gabriel
 * Date: 6/13/12
 */

class PlainFileDiskStore(f:File) extends DiskStore{

  val output=new FileOutputStream(f)

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    output.write(key)
    output.write(value)
  }
  def get(key: Array[Byte]): Option[Array[Byte]]= None //we're testing the write performance only
  def flush(): Unit = output.flush()

  def traverse[A](start: Array[Byte], end: Array[Byte])(reader: Reader[A]): A = sys.error("traverse not implemented")

}

object PlainFileDiskStoreSource extends DiskStoreSource {
  def apply(name: String): DiskStore = new PlainFileDiskStore(new File(name))
}