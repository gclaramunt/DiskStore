package diskstore.simple

import java.io.File
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeExample
import java.util.Date

/**
 * User: Gabriel
 * Date: 5/24/12
 */

class KeyStoreTest extends Specification with BeforeExample {

  val dir = new File("test/keytest")

  def before ={
    dir.mkdirs()
  }

  def storageName(s:String)="KEYS-%s-%s" format(s,new Date().getTime)


  "The key store write" should {
    "write and read a Ref" in {
      val ks=KeyStore(Buckets(dir,storageName("A"),8))
      ks.write(Array[Byte](1,3,4),Ref("A",12)) should not(throwA[Exception])
      ks.read(Array[Byte](1,3,4))=== Some(Ref("A",12))
    }

    "replace an existing value" in {
      val ks=KeyStore(Buckets(dir,storageName("B"),8))
      ks.write(Array[Byte](1,3,5),Ref("AC",132))
      ks.write(Array[Byte](1,3,5),Ref("BC",777))
      ks.read(Array[Byte](1,3,5))=== Some(Ref("BC",777))
    }

    "write values across buckets" in {
      val ks=KeyStore(Buckets(dir,storageName("C"),2))
      ks.write(Array[Byte](7,3,5),Ref("zx",333))
      ks.write(Array[Byte](2,4,5),Ref("yz",999))
      ks.read(Array[Byte](7,3,5))=== Some(Ref("zx",333))
      ks.read(Array[Byte](2,4,5))=== Some(Ref("yz",999))
    }
  }

  "loadBucket" should {
    "return the empty map when the bucket is not found" in {
      val ks=KeyStore(Buckets(dir,storageName("C"),2))
      ks.loadBucket("non-existent-bucket").isEmpty === true
    } // should not(throwA[Exception])

    "load the the bucket" in {
      val ks=KeyStore(Buckets(dir,storageName("D"),2))
      ks.write(Array[Byte](1,3,4),Ref("A",12))
      ks.write(Array[Byte](1,3,5),Ref("D",22))
      val bucketId=ks.buckets.bucketId(Array[Byte](1,3,5))
      val bucket=ks.loadBucket(bucketId)
      bucket.isEmpty === false
      bucket.size === 2
      bucket(Array[Byte](1,3,4))===Ref("A",12)
      bucket(Array[Byte](1,3,5))===Ref("D",22)

    }


  }



}
