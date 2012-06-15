package diskstore.simple

import scala.collection._
import java.io._
import scala.Array
import java.math.BigInteger
import diskstore.util.{IO, ByteArrayIO}
import annotation.tailrec


/**
 * User: gabriel
 * Date: 5/23/12
 */

case class Buckets(dir:File, name:String, bucketSizeFactor:Int) {

  //private[this] var bucketOSMap = Map[String, DataOutputStream]()
  val zero="ZERO"

  def bucketName(id:String)= "%s%s" format (name,id)

  def hexString(bs:Array[Byte])=bs.map( "%02X".format(_) ).mkString

  /**
   * Gets the string name of the bucket to store this key
   * (currently, the hex representation of key minus the last /bucketLength/ elements )
   * @param key byte array
   * @return a string
   */
  def bucketId(key:Array[Byte])=
    if (key.length <= bucketSizeFactor) zero
    else hexString(key.dropRight(bucketSizeFactor))

  /**
   * returns the next bucket in the natural order
   * @param bucket bucket name
   * @return next bucket name
   */
  def next(bucket:String)= if (bucket == zero) "0"
                  else hexString((new BigInteger(bucket,16)).add(BigInteger.ONE).toByteArray)

  def file(id: String) = {
    val file = new File(dir, bucketName(id))
    file.createNewFile()
    file
  }

  /*def bucketOutputStream(bId: String, forAppend:Boolean=false) = bucketOSMap.getOrElse(bId, {
    val newOS = IO.newDataOutputStream(file(bId),forAppend)
    bucketOSMap += (bId -> newOS)
    newOS
  })*/


  def flush() = {
    //bucketOSMap.values.map(_.flush())
    //recoveryMark()
  }

  def close() = {
    flush()
    //bucketOSMap.values.map(_.close())
    recoveryStrm.close()
  }

  val recoveryFile=new File(dir,".recovery-%s". format(name))
  val recoveryStrm = IO.newDataOutputStream(recoveryFile, forAppend = true)

  /*private def recoveryMark() {
    bucketOSMap.keys.foreach( k => {
        //write bucket
        ByteArrayIO.write(bucketName(k).getBytes,recoveryStrm)
        //write position on bucket
        recoveryStrm.writeLong(file(k).length)
      }
    )
    recoveryStrm.flush()
  }*/

  private def readRecoveryData()=  {

    @tailrec
    def readRecoveryRec(is:DataInputStream, bucketPos:Map[String,Long]):Map[String,Long] = {
      val posOfBucket:Option[(String,Long)] =
        for {
          bucket <- ByteArrayIO.read(is)
          pos    <- ByteArrayIO.maybeEof(Some(is.readLong()))
        } yield (new String(bucket) -> pos)
      //just for TCO
      if ( posOfBucket.isEmpty)
        bucketPos
      else
        readRecoveryRec(is, bucketPos + posOfBucket.get )
    }

    IO.withDataInputStream(recoveryFile) { readRecoveryRec(_, Map()) }
  }

  def recovery() = {
    val sizes = readRecoveryData()
    //optionally filter only the update date > recoveryFile update date
    sizes.foreach( truncateFile )
  }

  private def truncateFile(t:(String,Long)){
    val (bucket, size)=t
    val raf=new RandomAccessFile(bucket,"rws")
    raf.setLength(size)
    raf.close()
    //alternative... either way, neither seems to work :)
    //(I bet there's a stream already open)
    val outChan = new FileOutputStream(bucket, true).getChannel
    outChan.truncate(size)
    outChan.close()
  }

}
