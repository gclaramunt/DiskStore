package diskstore.simple

import java.io._
import org.specs2.mutable.Specification
import org.specs2.specification.{AfterExample, BeforeExample}

/**
 * User: Gabriel
 * Date: 5/27/12
 */

class BucketsTest extends Specification with BeforeExample with AfterExample {

  val dir = new File("test/BucketsTest")
  val n=2
  var buckets:Buckets = _

  def before {
    dir.mkdirs()
    buckets=Buckets(dir,"Buckets",n)
  }


  "bucketId" should {

    "generate the hex string from the byte array without the last n bytes" in {
      buckets.bucketId(Array[Byte](1,2,3,4))==="0102"
    }

    "generate zero if the array is less or equals than n" in {
      buckets.bucketId(Array[Byte](1,2))==="ZERO"
      buckets.bucketId(Array[Byte](12))==="ZERO"
      buckets.bucketId(Array[Byte]())==="ZERO"
    }

  }

  "next" should {
    "generate the next bucketId from a given one" in {
      buckets.next("0102")==="0103"
      buckets.next(buckets.bucketId(Array[Byte](2,3,3,4)))==="0204"
    }

    "generate the 0 bucketId from a ZERO bucketId" in {
      buckets.next("ZERO")==="0"
    }

  }
/*
  "bucketOutputStream" should {
    "create an output stream for a given bucket id" in {
      buckets.bucketOutputStream("1") must beAnInstanceOf[OutputStream]
    }

    "reuse the same output stream for the same bucket id" in {
      buckets.bucketOutputStream("1") === buckets.bucketOutputStream("1")
    }
  }*/

  "flush" should {
    "not throw an exception" in {
      buckets.flush() should not( throwAn[Exception])
    }
  }

  def after {
    buckets.close()
    dir.listFiles().foreach( _.delete())
  }

}
