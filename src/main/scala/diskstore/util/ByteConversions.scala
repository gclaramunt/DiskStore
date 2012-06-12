package diskstore.util

import java.nio.ByteBuffer
import diskstore.simple.Ref

/**
 * User: gabriel
 * Date: 5/25/12
 */

object ByteConversions {

  implicit def refToArr(r:Ref):Array[Byte]= {
    val bucketArray= r.bucket.getBytes
    val b=ByteBuffer.allocate(bucketArray.size+8).putLong(r.pos).put(bucketArray).array()
    b
  }

  implicit def arrToRef(arr:Array[Byte]):Ref={
    val buffer=ByteBuffer.wrap(arr)
    val pos= buffer.getLong
    val slice= buffer.slice()
    val strArr=new Array[Byte](slice.array.size-slice.arrayOffset)
    slice.get(strArr)
    val key=new String(strArr)
    Ref(key,pos)
  }


}
