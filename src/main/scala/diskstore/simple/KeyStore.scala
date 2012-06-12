package diskstore.simple

import scala.Array
import java.io._
import collection.immutable.SortedMap
import diskstore.util.ByteConversions._
import annotation.tailrec
import diskstore.util.{ByteArrayIO, IO,SpecializedByteArrayOrdering}


/**
 * User: gabriel
 * Date: 5/23/12
 */

case class KeyStore(buckets:Buckets) {

  def write(key:Array[Byte],ref:Ref){
    val os= buckets.bucketOutputStream(buckets.bucketId(key),forAppend=true)
    ByteArrayIO.write(key,os)
    ByteArrayIO.write(ref,os)
  }

  def loadBucket(bucketId:String):SortedMap[Array[Byte],Ref]={

    //TODO generalize data input stream recursion into (another) iteratee (see Buckets.readRecoveryRec )
    @tailrec
    def loadBucketRec(bucket:SortedMap[Array[Byte],Ref],dis:DataInputStream):SortedMap[Array[Byte],Ref] = {
      val key=ByteArrayIO.read(dis)
      val ref=ByteArrayIO.read(dis)   //if eof, done
      if (key == None && ref == None) bucket
      else if (key == None || ref == None) sys.error("got out of synch with key %s ref %s " format(key,ref))
      else {
        loadBucketRec(bucket+(key.get->ref.get),dis)
      }
    }

    val bucket=SortedMap[Array[Byte],Ref]()(SpecializedByteArrayOrdering)
    IO.withDataInputStream(buckets.file(bucketId)){ loadBucketRec(bucket,_) }

  }

  def read(key:Array[Byte]):Option[Ref] = loadBucket(buckets.bucketId(key)).get(key)

}
