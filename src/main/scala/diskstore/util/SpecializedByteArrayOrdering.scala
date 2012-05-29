package diskstore.util

/**
 * User: gabriel
 * Date: 5/23/12
 */


/**
 * Specialized Array[Byte] ordering  (and successor)
 */
object SpecializedByteArrayOrdering extends Ordering[Array[Byte]] {




  /**
   * Compares two byte arrays
   * adapted from
   * http://stackoverflow.com/questions/7109943/how-to-define-orderingarraybyte
   * ( Although it could be replaced with BigInteger(a).compare(BigInteger(b)) )
   * @param a byte array
   * @param b byte array
   * @return -1 if a<b, 0 if a=b, 1 if b<a
   */
  def compare(a: Array[Byte], b: Array[Byte]): Int = {
    if (a eq null) {
      if (b eq null) 0
      else -1
    }
    else if (b eq null) 1
    else if (a.length < b.length) -1
    else if (b.length < a.length) 1
    else {
      val L = math.min(a.length, b.length)
      var i = 0
      while (i < L) {
        if (a(i) < b(i)) return -1
        else if (b(i) < a(i)) return 1
        i += 1
      }
      0
    }
  }



}
