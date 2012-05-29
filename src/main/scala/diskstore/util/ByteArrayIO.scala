package diskstore.util

import scala.Array
import java.io._

/**
 * User: Gabriel
 * Date: 5/23/12
 *
 * Arrays are stored length (byte), data (byte*)
 */

object IO{


  private def newDataInputStream(file: File) =
    new DataInputStream(new BufferedInputStream(new FileInputStream(file)))

  /**
   * Executes a read function with a DataInputStream and then closes it
   * @param file file to read
   * @param read function from DataInputStream => T that reads the stream
   * @tparam T result type of the read
   * @return result of the read function
   */
  def withDataInputStream[T](file: File)(read: DataInputStream => T): T = {
    val dis = newDataInputStream(file)
    try {
      read(dis)
    } finally {
      dis.close()
    }
  }
}

object ByteArrayIO {




  /**
   * Writes a byte array into the stream, writing the size first and returning the total bytes written
   * @param data array of bytes to write
   * @param writeStream target output stream
   * @return total bytes written
   */
  def write(data: Array[Byte], writeStream: DataOutput): Int = {
    writeStream.writeInt(data.size)
    writeStream.write(data)
    data.length + 4
  }

  /**
   * Reads a byte array from the stream, reading the size first or none if cannot be read
   * @param readStream input stream
   * @return Optional byte array
   */
  def read(readStream: DataInputStream): Option[Array[Byte]] = {
    try {
      val size = readStream.readInt()
      if (size > 0) {
        val data = new Array[Byte](size)
        if (readStream.read(data) == -1) None
        else Some(data)
      } else None
    } catch {
      case e: EOFException => None
    }
  }


}

