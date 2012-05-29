package diskstore.simple

import scala.collection._
import java.io._
import scala.Array
import java.math.BigInteger


/**
 * User: gabriel
 * Date: 5/23/12
 */

case class Buckets(dir:File, name:String, bucketSizeFactor:Int) {

  private[this] var bucketOSMap = mutable.Map[String, DataOutputStream]()
  val zero="ZERO"

  def bucketName(id:String)= "%s%s" format (name,id)

  /**
   * Gets the string name of the bucket to store this key
   * (currently, the hex representation of key minus the last /bucketLength/ elements )
   * @param key byte array
   * @return a string
   */
  def bucketId(key:Array[Byte])=
    if (key.length <= bucketSizeFactor) zero
    else "%X".format(new BigInteger(key.dropRight(bucketSizeFactor)))

  /**
   * returns the next bucket in the natural order
   * @param bucket bucket name
   * @return next bucket name
   */
  def next(bucket:String)=
    "%X".format ( if (bucket == zero) BigInteger.ZERO
                  else ((new BigInteger(bucket,16)).add(BigInteger.ONE)))

  def file(id: String) = {
    val file = new File(dir, bucketName(id))
    file.createNewFile()
    file
  }

  def bucketOutputStream(bId: String, forAppend:Boolean=false) = bucketOSMap.getOrElse(bId, {
    val newOS = new DataOutputStream(new FileOutputStream(file(bId),forAppend))
    bucketOSMap += (bId -> newOS)
    newOS
  })

  def flush() = bucketOSMap.values.map(_.flush())

  def close() = bucketOSMap.values.map(_.close())

}
