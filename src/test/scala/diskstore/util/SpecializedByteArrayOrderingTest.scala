package diskstore.util

import org.specs2.mutable.Specification
import diskstore.util.SpecializedByteArrayOrdering._


/**
 * User: gabriel
 * Date: 5/24/12
 */

class SpecializedByteArrayOrderingTest extends Specification {


  "compare" should {
    "null is equal to null " in {
      equiv(null, null)
    }

    "null is less than any array " in {
      lt(null, Array[Byte]())
    }

    "any array is greater than null" in {
      gt(Array[Byte](), null)
    }

    "any array is smaller than a longer one" in {
      lt(Array[Byte](127), Array[Byte](1, 1))
    }

    "any array is greater than a shorter one" in {
      gt(Array[Byte](12, 1, 3), Array[Byte](13, 100))
    }

    "same length arrays are compared by values" in {
      equiv(Array[Byte](12, 1, 3), Array[Byte](12, 1, 3)) === (true)
      gt(Array[Byte](12, 4, 3), Array[Byte](12, 1, 3)) === (true)
      gt(Array[Byte](12, 4, 3), Array[Byte](12, 1, 7)) === (true)
    }

  }

}
