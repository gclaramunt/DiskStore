package diskstore.simple

import org.specs2.mutable.Specification
import diskstore.{Reader, More, Done}


/**
 * User: gabriel
 * Date: 5/24/12
 */

class SimpleDiskStoreTest extends Specification {

  "The SimpleDiskStore put" should {

    "reject the null value" in {
      val store =SimpleDiskStoreSource("test/diskstore-test/test-1")
      store.put(Array[Byte](1),null) should throwA[Exception]
    }

    "store a value" in {
      val store =SimpleDiskStoreSource("test/diskstore-test/test-2")
      store.put(Array(1.toByte),Array(2.toByte))
      store.get(Array(1.toByte)).get===(Array(2.toByte))
    }
    "store different values" in {
      val store =SimpleDiskStoreSource("test/diskstore-test/test-3")
      store.put(Array(1.toByte),Array(2.toByte))
      store.put(Array(3.toByte),Array(4.toByte))
      store.get(Array(1.toByte)).get===(Array(2.toByte))
      store.get(Array(3.toByte)).get===(Array(4.toByte))
    }

    "replace a stored value" in {
      val store =SimpleDiskStoreSource("test/diskstore-test/test-4")
      store.put(Array(1.toByte),Array(2.toByte))
      store.get(Array(1.toByte)).get===(Array(2.toByte))
      store.put(Array(1.toByte),Array(3.toByte))
      store.get(Array(1.toByte)).get===(Array(3.toByte))
    }
  }

  "The SimpleDiskStore get" should {

    /*
    // Alternate null behaviour
    "throw an exception for the null key" in {
      store.get(null) should throwA[Exception]
    }
    */

    "Return none if the key is not present" in {
      val store =SimpleDiskStoreSource("test/diskstore-test/test-5")
      store.get(Array(123.toByte)) should be(None)
    }
  }

  //iteratee to just collect the results
  def fold(xs:List[Array[Byte]])(mkv:Option[(Array[Byte], Array[Byte])]):Reader[List[Array[Byte]]]=
    mkv match {
      case None => Done(xs)
      case Some(kv) =>  More(fold(kv._2 :: xs) _ )
    }

  "Iterate through range" should {
    "Iterate through the same bucket" in {
      val store =SimpleDiskStoreSource("test/diskstore-test/test-bucket",2,2)

      store.put(Array[Byte](1,2,3,4),Array[Byte](3,4,5))
      store.put(Array[Byte](1,2,3,7),Array[Byte](1,3,2))
      store.put(Array[Byte](1,2,4,10),Array[Byte](1,3,2))

      store.traverse(Array[Byte](1,2,3,4),Array[Byte](1,2,4,11))(More(fold(Nil)_)).map(_.mkString) must haveTheSameElementsAs ((Array[Byte](1,3,2) :: Array[Byte](1,3,2) ::Array[Byte](3,4,5) :: Nil).map(_.mkString))

    }

    "Iterate across buckets" in {
      val store =SimpleDiskStoreSource("test/diskstore-test/test-buckets",2,2)

      store.put(Array[Byte](1,2,3,4),Array[Byte](3,4,5))
      store.put(Array[Byte](1,7,3,9),Array[Byte](1,3,2))
      store.put(Array[Byte](10,34,4,10),Array[Byte](1,3,2))

      store.traverse(Array[Byte](1,2,3,4),Array[Byte](12,22,4,11))(More(fold(Nil)_)).map(_.mkString) must haveTheSameElementsAs ((Array[Byte](1,3,2) :: Array[Byte](1,3,2) ::Array[Byte](3,4,5) :: Nil).map(_.mkString))

    }
  }

  "flush" should {
    "flush the output streams" in {
      val store =SimpleDiskStoreSource("test/diskstore-test/test-buckets",2,2)
      store.flush() should not(throwA[Exception])
    }
  }

  "recovery" should {
    "store different values" in {

      //
      new Object{
        val store =SimpleDiskStoreSource("test/diskstore-test/test-recovery")
        store.put(Array(1.toByte),Array(2.toByte))
        store.put(Array(3.toByte),Array(4.toByte))
        store.flush()
        store.put(Array(5.toByte),Array(3.toByte))

        //let's blow up
        store.put(Array[Byte](1),null) should throwA[NullPointerException]
      }

      val store =SimpleDiskStoreSource("test/diskstore-test/test-recovery")
      store.get(Array(1.toByte)).get===(Array(2.toByte))
      store.get(Array(3.toByte)).get===(Array(4.toByte))
      //store.get(Array(5.toByte))===None
    }
  }



}
