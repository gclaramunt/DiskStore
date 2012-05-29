package diskstore.simple

import collection.immutable.SortedMap
import diskstore._
import diskstore.util.SpecializedByteArrayOrdering._
import annotation.tailrec
import java.io.File


/**
 * User: Gabriel
 * Date: 5/22/12
 *
 * Assumptions
 * data arrays no longer than max_int (not hard to change anyway)
 *
 */

class SimpleDiskStore(name:String,keysBucketFactor:Int=8,valuesBucketFactor:Int=8 ) extends DiskStore {

  val dir = new File(name)
  dir.mkdirs()

  private val keyStore = KeyStore(Buckets(dir,"KEYS",keysBucketFactor))
  private val valueStore= ValuesStore(Buckets(dir,"VALUES",valuesBucketFactor))


  def put(key: Array[Byte], value: Array[Byte]) {
    //find the value or store it and get a pointer to it
    val ref=valueStore.find(value).getOrElse(valueStore.write(value))
    keyStore.write(key,ref)
  }

  def get(key: Array[Byte]): Option[Array[Byte]] = keyStore.read(key).flatMap(pos =>valueStore.read(pos))

  def flush() {
    valueStore.flush()
    keyStore.buckets.flush()
  }

  def close() {
    valueStore.close()
    keyStore.buckets.close()
  }

  /*
  case class More[A](value: Option[(Array[Byte], Array[Byte])] => Reader[A]) extends Reader[A]
  case class Done[A](value: A) extends Reader[A]
  */


  /**
   * Traverse the storage in the range from start until end with a supplied iteratee
   * @param start from (inclusive)
   * @param end until (exclusive)
   * @param reader Iteratee to collect the key/values
   * @tparam A type of the iteratee
   * @return value of the end of the iteratee run
   */
  def traverse[A](start: Array[Byte], end: Array[Byte])(reader: Reader[A]):A ={
    ( traverseRec(start,end, keyStore.buckets.bucketId(start))(reader)) match {
      case d:Done[A] => d.value
      case _ => sys.error("expecting done from iteratee")
    }
  }

  private def loadBucketValues(b:SortedMap[Array[Byte],Ref])=
    b.map( kv => valueStore.read(kv._2).map(kv._1 -> _) )

  @tailrec
  private def traverseRec[A](start: Array[Byte], end: Array[Byte], currentBucketId:String )(reader: Reader[A]):Reader[A] =
    reader match {
      case r:More[A] =>{
        val keys = keyStore.loadBucket(currentBucketId)
        val iteratee=loadBucketValues(keys.range(start,end))
          .foldLeft(reader)((r,kv)=> r match {
          case More(value) if (kv != None) => value(kv)
          case _ => r
        })
        iteratee match {
          case more:More[A] =>
            if ( currentBucketId <=  keyStore.buckets.bucketId(end) && ( keys.isEmpty || keys.lastKey < end))
              traverseRec[A](start,end,keyStore.buckets.next(currentBucketId))(more)
            else
              more.value(None) //tell the Iteratee we're done
          case _ => iteratee
        }

      }
      case _ => reader
    }
    
}

object SimpleDiskStoreSource extends DiskStoreSource {
  def apply(name: String)= new SimpleDiskStore(name)
  def apply(name: String,keysBucketFactor:Int=8,valuesBucketFactor:Int=8 )= new SimpleDiskStore(name,keysBucketFactor,valuesBucketFactor)
}
