package diskstore.simple

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeExample
import java.io.File
import java.util.Date

/**
 * User: Gabriel
 * Date: 5/25/12
 */

class ValuesStoreTest extends Specification with BeforeExample {

  var vs:ValuesStore=_
  val dir = new File("test/ValueTest")

  def before ={
    dir.mkdirs()
  }

  def storageName(s:String)="VALUES-%s-%s" format(s,new Date().getTime)

  "The value store write" should {
    "write and read an array in the start position of the bucket" in {
      val vs=ValuesStore(Buckets(dir,storageName("a1"),8))
      val ref=vs.write(Array[Byte](64,65,66))
      ref.pos===0
      vs.read(ref).get === Array[Byte](64,65,66)
    }

    "write and read an array in the start position of the bucket" in {
      val vs=ValuesStore(Buckets(dir,storageName("a2"),8))
      val ref=vs.write(Array[Byte](64,65,66))
      ref.pos===0
      vs.read(ref).get === Array[Byte](64,65,66)
    }
  }

  "The value store find" should {
    "find an array in the start position" in  {
      val vs=ValuesStore(Buckets(dir,storageName("b1"),8))
      val ref=vs.write(Array[Byte](64,65,66))
      vs.find(Array[Byte](64,65,66)) === Some(ref)
    }

    "find an array later in the file" in  {
      val vs=ValuesStore(Buckets(dir,storageName("b2"),8))
      vs.write(Array[Byte](64,65,66))
      val ref=vs.write(Array[Byte](20,21,20))
      vs.find(Array[Byte](20,21,20)) === Some(ref)
    }
  }


}

