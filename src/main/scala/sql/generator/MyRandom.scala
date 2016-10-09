package sql.generator

import scala.util.Random
/**
  * Created by rui on 16-9-23.
  */
object MyRandom {

  def randomInt(a:Int,b:Int): Int ={
    return a+Random.nextInt(b-a+1)
  }

  def randomUrlname(namelen:Int): String ={
    var name = ""
    for (i <- 1 to namelen){
      if (Random.nextInt(2) == 0)
        name = name + (Random.nextInt(26)+'A'.toInt).toChar
      else
        name = name + (Random.nextInt(26)+'a'.toInt).toChar
    }
    return name
  }
  def randomDate(): Long ={
    val sec = 1000000000 + (Random.nextFloat()*(System.currentTimeMillis()-1000000000)).toLong
    return sec
  }
  def randomFloat(x:Float): Float ={
    return Random.nextFloat() * x
  }
  def randomBase(): Float={
    return Random.nextFloat()
  }
  def randomNormal(): Double ={
    var s = 2.0
    var v1,v2,u1,u2 = 0.0
    while (s>=1 || s == 0){
      u1 = Random.nextDouble()
      u2 = Random.nextDouble()
      v1 = 2.0 * u1 - 1.0
      v2 = 2.0 * u2 - 1.0
      s = v1 * v1 + v2 * v2
    }
    var x1 = v1 * math.sqrt(-2.0*math.log(s)/s)
    return x1
  }

}
