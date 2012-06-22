package diskstore.buffered

import diskstore.{Reader, DiskStoreSource, DiskStore}

/**
 * User: gabriel
 * Date: 6/22/12
 */

class BufferedDiskStore(name:String,keysBucketFactor:Int=8,valuesBucketFactor:Int=8 ) extends DiskStore {

  var buffer=Map[Array[Byte],Array[Byte]]()

  def put(key: Array[Byte], value: Array[Byte]) {buffer += (key-> value)}
  def get(key: Array[Byte]): Option[Array[Byte]] = buffer.get(key) //or else look in the file system
  def flush(){
    // this is where it gets stored

    //buffer.values.groupBy(buckets.bucketId(_))
    //group values by bucket
    //write each bucket, return a map of refs(bucket, pos)
    //group keys by bucket
    //write buckets of keys with refs

    // ???
    // profit

  }

  def traverse[A](start: Array[Byte], end: Array[Byte])(reader: Reader[A]): A = sys.error("not implemented")//let's use the simplediskstore one


}

object BufferedDiskStoreSource extends DiskStoreSource {
  def apply(name: String)= new BufferedDiskStore(name)
  def apply(name: String,keysBucketFactor:Int=8,valuesBucketFactor:Int=8 )= new BufferedDiskStore(name,keysBucketFactor,valuesBucketFactor)
}
