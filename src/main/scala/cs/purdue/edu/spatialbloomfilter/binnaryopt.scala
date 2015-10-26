package cs.purdue.edu.spatialbloomfilter

import cs.purdue.edu.spatialindex.rtree.Box


/**
 * data is organized as this way:
 *[1000 1000] [0001 0001]
 * each 32 bit, the data is organized from low to high
 * for example
 * [1000 1000 0001 1000 1111], 1111 is the root node, then next children is 1000, 0001, 1000, 1000
 * once one integer is filled, we go to the next integer and fill from low position to high
 */

/**
 * Created by merlin on 10/14/15.
 */
object binnaryopt {

  def SetBit(k:Int, data:Array[Int])={
    val i=k/32
    val pos=k%32

    var flag=1
    flag=flag<<pos

    data(i)=data(i)|flag

  }

  /**
   * set the
   * @param beginK
   * @param endK
   * @param data
   */
  def SetBit(beginK:Int, endK:Int, setbyte:Int, data:Array[Int])=
  {
      if(endK-beginK<=qtreeUtil.binnaryUnit&&setbyte<=qtreeUtil.binnaryMax)
      {

        var setdata=setbyte //1011
        val i=beginK/32
        val pos=beginK%32

        setdata=setdata<<pos // 1011 0000 if pos=4

        data(i)=data(i)|setdata // data(i)=0001 0011
                                // setbit= 1011 0000 then, data(i)= 1011 0011
      }
  }


  def ClearBit(k:Int, data:Array[Int])={

    val i=k/32
    val pos=k%32

    var flag=1
    flag=flag<<pos

    flag = ~flag

    data(i)=data(i)&flag

  }

  def GetBit(k:Int, data:Array[Int]):Int={

    val i=k/32
    val pos=k%32

    var flag=1
    flag=flag<<pos

    if((data(i)&flag)!=0)
      1
    else
      0
  }

  def getBit(k:Int, data:Int):Int={

    val pos=k%32

    var flag=1
    flag=flag<<pos

    if((data&flag)!=0)
      1
    else
      0
  }

  def setBit(k:Int, data:Int):Int={

    val pos=k%32

    var flag=1
    flag=flag<<pos

    flag=data|flag

    flag
  }

  def clearBit(k:Int, data:Int):Int={

    //val i=k/32
    val pos=k%32

    var flag=1
    flag=flag<<pos

    flag = ~flag

    flag=data&flag

    flag
  }

  def GetBytes(beginK:Int, endK:Int, data:Array[Int]):Int={

    if(endK-beginK<=qtreeUtil.binnaryUnit)
    {
      val i=beginK/32
      val pos1=beginK%32
      //val pos2=endK%32

      var flag=16 //1111, we assume this
      flag=flag<<pos1  //0000 0000 1111 0000 if pos1=4

      var tmpdata=data(i) //1011 1101 1011 1101

      tmpdata =tmpdata&flag //0000 0000 1011 0000

      tmpdata>>pos1 // return 1011
    }else
    {
      //there is error here
      //println("out of boundary")
      0
    }

  }

  /**
   * get the bitcount for data inside the range
   * this can be effecient, since get the bit count for one integer only taking 12 operations
   * @param begin
   * @param end
   * @return
   */
  def getSetcount(begin:Int, end:Int, data:Array[Int]):Int={

    val i=begin/32
    val pos1=begin%32

    val j=end/32
    val pos2=end%32

    var tmpdata=data

    var count=0

    for(m<-i to j)
    {
      if(m==i)
      {
        count=count+bitcount(data(m)>>pos1)
      }else if(m==j)
      {
        count=count+bitcount(data(m)<<(32-pos2))

      }else
      {
        count=count+bitcount(data(m))
      }

    }

    count
  }


  /**
   * count number of set bit in one data
   * @param data
   * @return
   */
  def bitcount(data:Int):Int={

    var i=data

    i = i - ((i >> 1) & 0x55555555)

    i = (i & 0x33333333) + ((i >> 2) & 0x33333333)

    (((i + (i >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24

  }


}

object qtreeUtil{

  def rangx=100
  def rangy=100
  def leafcount=100
  final def binnaryUnit=4
  final def binnaryMax=16

  def wholespace=Box(0,0,8f,8f)

  def less(Key1:Float, Key2:Float):Boolean=
  {
    Key1.compareTo(Key2) <  0
  }

  def insideBox(small:Box, big:Box): Boolean =
  {

    if(small.x>big.x&&small.y>big.y&&small.x2<big.x2&&small.y2<big.y2)
      true
    else
      false

  }

}
