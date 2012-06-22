package diskstore.util

import org.specs2.mutable.Specification
import java.math.BigInteger
import org.specs2.ScalaCheck

/**
 * User: gabriel
 * Date: 6/19/12
 */

class ByteToHexTest extends Specification with ScalaCheck {
  override def is =
  "hex conversion" ! check { (b:Byte) => b==(new BigInteger(ByteToHex(b),16)).byteValue()}

}
