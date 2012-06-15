package diskstore

import scala.util.Random
import simple.SimpleDiskStoreSource
import java.math.BigInteger

/**
 * User: Gabriel
 * Date: 6/13/12
 */

object PerformanceBenchmark {

  def main(args:Array[String]){
    val timeRandom=compareStores(x=>randomData(100)) _
    val timeSeq=compareStores(sequencedData _) _

    timeRandom(1000)
    timeRandom(10000)
    //timeRandom(10000000)

    timeSeq(1000)
    timeSeq(10000)
    //timeSeq(10000000)
  }

  def random(size:Int)={
    val d=new Array[Byte](size)
    Random.nextBytes(d)
    d
  }

  def randomData(size:Int)=(random(size),random(size))

  def sequencedData(i:Int)={
    val arr=intToBytes(i)
    (arr,arr)
  }
  def intToBytes(i:Int)= (new BigInteger(i.toString)).toByteArray

  def write(dataFn: Int=>(Array[Byte],Array[Byte]))( n:Int)( d:DiskStore){
    var i=0
    while (i< n){
      val (k,v)=dataFn(i)
      d.put(k,v)
      i+=1
    }
  }

  def time(f: =>Unit)={
    val start=System.currentTimeMillis()
    f
    val end=System.currentTimeMillis()
    end-start
  }

  def compareStores(dataFn: Int=>(Array[Byte],Array[Byte]))(runs:Int){
    val writefn= write(dataFn)(runs) _
    val sds=SimpleDiskStoreSource("perf/ds-perf-test-%s" format runs)
    val sdstime=time(writefn(sds))
    sds.close()

    val pfds=PlainFileDiskStoreSource("perf/plain-file-%s" format runs)
    val pfdstime=time(writefn(pfds))
    pfds.close()

    println("Simple Disk Store time %s" format(sdstime))
    println("Plain File Disk Store time %s" format(pfdstime))
  }




}
