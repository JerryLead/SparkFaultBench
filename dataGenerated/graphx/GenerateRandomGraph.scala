package algorithms

import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by test on 2016/8/20.
  */
object GenerateRandomGraph {

  /**
    * To run this program use the following:
    *
    * bin/spark-submit --class algorithms.GenerateRandomGraph \
    * --master yarn \
    * --deploy-mode cluster \
    * /home/hadoop/zw/GenerateRandomGraph.jar -nverts=10000
    *
    * Options:
    *   -nverts the number of vertices in the graph (Default: 10000)
    *   -numEPart the number of edge partitions in the graph (Default: number of cores)
    *   -partStrategy the graph partitioning strategy to use
    *   -mu the mean parameter for the log-normal graph (Default: 4.0)
    *   -sigma the stdev parameter for the log-normal graph (Default: 1.3)
    *   -seed seed to use for RNGs (Default: -1, picks seed randomly)
    *   -vofpath vertice output file path (Default: data/zw/vertice)
    *   -eofpath edge onput file path (Default: data/zw/edge)
    */

  def main(args: Array[String]): Unit = {

    val options = args.map {
      arg =>
        arg.dropWhile(_ == '-').split('=') match {
          case Array(opt, v) => (opt -> v)
          case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
        }
    }

    var numVertices = 10000
    var numEPart: Option[Int] = None
    var mu: Double = 4.0
    var sigma: Double = 1.3
    var seed: Int = -1
    var vofpath: String = "data/zw/vertice"
    var eofpath: String = "data/zw/edge"

    options.foreach {
      case ("nverts", v) => numVertices = v.toInt
      case ("numEPart", v) => numEPart = Some(v.toInt)
      case ("mu", v) => mu = v.toDouble
      case ("sigma", v) => sigma = v.toDouble
      case ("seed", v) => seed = v.toInt
      case ("vofpath", v) => vofpath = v
      case ("eofpath", v) => eofpath = v
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
    GraphXUtils.registerKryoClasses(conf)

    val sc = new SparkContext(conf)

    // Create the graph
    println(s"Creating graph...")
    val unpartitionedGraph = GraphGenerators.logNormalGraph(sc, numVertices,
      numEPart.getOrElse(sc.defaultParallelism), mu, sigma, seed)

    // Save the result
    unpartitionedGraph.vertices.mapValues((id, _) => "p" + id).sortByKey().saveAsTextFile(vofpath)
    unpartitionedGraph.edges.saveAsTextFile(eofpath)

    sc.stop()
  }
}