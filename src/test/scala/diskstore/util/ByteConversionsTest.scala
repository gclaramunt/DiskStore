package diskstore.util

import org.specs2.mutable.Specification
import diskstore.util.ByteConversions._
import diskstore.simple.Ref

/**
 * User: gabriel
 * Date: 5/28/12
 */

class ByteConversionsTest extends Specification {

  "ref to and from array conversion" should {

    "serialize to byte array" in {
      refToArr(Ref("AB", 12)) === Array[Byte](0, 0, 0, 0, 0, 0, 0, 12, 65, 66)
      refToArr(Ref("ABC", 725)) === Array[Byte](0, 0, 0, 0, 0, 0, 2, -43, 65, 66, 67)
      refToArr(Ref("Z", 0)) === Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 90)
      refToArr(Ref("", 0)) === Array[Byte](0, 0, 0, 0, 0, 0, 0, 0)
      refToArr(Ref("ZX", 333)) === Array[Byte](0, 0, 0, 0, 0, 0, 1, 77, 90, 88)
    }

    "convert from byte array to ref" in {
      arrToRef(Array[Byte](0, 0, 0, 0, 0, 0, 0, 12, 65, 66)) === Ref("AB", 12)
      arrToRef(Array[Byte](0, 0, 0, 0, 0, 0, 2, -43, 65, 66, 67)) === Ref("ABC", 725)
      arrToRef(Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 90)) === Ref("Z", 0)
      arrToRef(Array[Byte](0, 0, 0, 0, 0, 0, 0, 0)) === Ref("", 0)
      arrToRef(Array[Byte](0, 0, 0, 0, 0, 0, 1, 77, 90, 88)) === Ref("ZX", 333)
    }

    "if I convert back and forth from array I should get the origiganl value" in {
      arrToRef(refToArr(Ref("AB", 12))) === Ref("AB", 12)
      arrToRef(refToArr(Ref("ABC", 725))) === Ref("ABC", 725)
      arrToRef(refToArr(Ref("Z", 0))) === Ref("Z", 0)
      arrToRef(refToArr(Ref("", 0))) === Ref("", 0)
      arrToRef(refToArr(Ref("ZX", 333))) === Ref("ZX", 333)
    }


  }

}
