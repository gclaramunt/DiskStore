package diskstore.simple

import annotation.tailrec
import java.io._
import collection._
import diskstore.util.{SpecializedByteArrayOrdering, ByteArrayIO, IO}

/**
 * User: gabriel
 * Date: 5/23/12
 */

case class Ref(bucket:String,pos:Long)

case class ValuesStore(buckets:Buckets) {

  var positions = Map[String, Long]()

  /**
   * Writes the length and the data of the array in the current pos,
   * returning the starting position of the data in the file
   * @param data byte array
   * @return Ref
   */
  def write(data:Array[Byte]):Ref={
    val bucketId = buckets.bucketId(data)
    IO.withDataOutputStream(buckets.file(bucketId)){ writeStream =>
      val insertPos = positions.getOrElse(bucketId, {
        val length=buckets.file(bucketId).length()
        positions+=(bucketId->length)
        length
      })
      val writtenBytes=ByteArrayIO.write(data,writeStream)
      positions+=(bucketId->writtenBytes)
      Ref(bucketId,insertPos)
    }
  }

  /**
   * Reads a byte array from a Reference (bucket+position)
   * @param ref Ref
   * @return Option of byte array
   */
  def read(ref:Ref):Option[Array[Byte]] =
    IO.withDataInputStream(buckets.file(ref.bucket))(dis=> {
      dis.skip(ref.pos)
      ByteArrayIO.read(dis)
    })

  /**
   * Finds the reference (bucket + position) of an array of bytes in the value storage
   * @param data byte array
   * @return option ref
   */
  def find(data:Array[Byte]):Option[Ref]={

    val bucketId=buckets.bucketId(data)

    @tailrec
    def findRec(currentPos:Long,data:Array[Byte], findStream:DataInputStream):Option[Ref]= {
      val read =ByteArrayIO.read(findStream)

      read match {
        case Some(found) if (SpecializedByteArrayOrdering.compare(data,found) == 0) => Some(Ref(bucketId,currentPos))
        case Some(found) => findRec(currentPos+found.length+4,data, findStream)
        case _ => None
      }
    }

    IO.withDataInputStream(buckets.file(bucketId))(dis => findRec(0,data,dis))
  }

}
