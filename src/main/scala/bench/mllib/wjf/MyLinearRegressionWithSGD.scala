package bench.mllib.wjf

import breeze.linalg.{DenseMatrix, DenseVector,linspace}
import breeze.plot._
/**
  * Created by wjf on 2016/8/23.
  */
object MyLinearRegressionWithSGD {
  def main(args: Array[String]): Unit ={
    val s=Seq(1,2,3)
    val a = new DenseVector[Int](1 to 3 toArray)
    val b =new DenseMatrix[Int](3,3,1 to 9 toArray)

    val f =Figure()
    val p = f.subplot(0)
    val x = linspace(0.0,1.0)
    p += plot(x ,x:^ 2.0)
    p += plot(x ,x:^ 3.0,'.')
    p.xlabel ="x axis"
    p.ylabel ="y axis"
    f.saveas("d:\\lines.png")
  }
}
