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

  def setBit(k:Int, data:Array[Int])={
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
  def setBit(beginK:Int, endK:Int, setbyte:Int, data:Array[Int])=
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


  def clearBit(k:Int, data:Array[Int])={

    val i=k/32
    val pos=k%32

    var flag=1
    flag=flag<<pos

    flag = ~flag

    data(i)=data(i)&flag

  }

  def getBit(k:Int, data:Array[Int]):Int={

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

  def getBytes(beginK:Int, endK:Int, data:Array[Int]):Int={

    if(endK-beginK<=qtreeUtil.binnaryUnit)
    {
      val i=beginK/32
      val pos1=beginK%32
      //val pos2=endK%32

      var flag=15 //1111, we assume this
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


    if(begin==end)
      return 0

    val i=begin/32
    val pos1=begin%32

    val j=end/32
    val pos2=end%32

    var tmpdata=data

    var count=0

    /*println("data is: ")
    println(binnaryopt.getBitString(begin,end,data))
    println("pos2 "+pos2)*/

    //those bit belong to only one integer
    if(i==j)
    {
      return  bitcount((data(i)>>>pos1)<<(32-pos2+pos1))
    }

    //wtf
    for(m<-i to j)
    {
      if(m==i)
      {
        count=count+bitcount(data(m)>>>pos1)
        //println("i  count=count+bitcount(data(m)>>pos1)"+binnaryopt.getBitString(data(m)>>>pos1))
      }else if(m==j)
      {
        if(pos2!=0)
        {
          //println("j before"+ binnaryopt.getBitString(data(m)))
          //println("j "+binnaryopt.getBitString(data(m)<<(32-pos2)))
          count=count+bitcount(data(m)<<(32-pos2))
        }

      }else
      {
        count=count+bitcount(data(m))
      }

      //println("m: "+m+" "+ count)
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

  def getBitString(begin:Int, end:Int, data:Array[Int]):String={


    var ret:String=new String()
    for(i<-begin to end-1)
    {
       if(this.getBit(i,data)==1)
       {
         ret+="1"
         //print(1)
       }else
       {
         //print(0)
         ret+="0"
       }
    }

    ret
  }

  def getBitString(data:Int):String={

    var ret:String=new String()

    for(i<-0 to 31)
    {
      if(this.getBit(i,data)==1)
      {
        ret+="1"
        //print(1)
      }else
      {
        //print(0)
        ret+="0"
      }
    }

    ret
  }

}

object qtreeUtil{

  final def rangx=10000
  final def rangy=10000

  final def leafbound=50

  def leafcount=100

  final def binnaryUnit=4
  final def binnaryMax=16

  //this bound is used for coalign testing between two lines
  final def errorbound=0.1

  //this bound is used to stop spilit the current node
  final def leafStopBound=0.1

  def wholespace=Box(0,0,rangx,rangy)

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

  def getAreaRatio(small:Box, big:Box):Double=
  {
    small.area/big.area
  }

}


