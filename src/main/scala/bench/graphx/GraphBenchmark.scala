package algorithms

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * Created by test on 2016/8/22.
  */
object GraphBenchmark {

  /**
    * To run this program use the following:
    *
    * bin/spark-submit --class algorithms.GraphBenchmark \
    * --master yarn \
    * --deploy-mode cluster \
    * /home/hadoop/zw/GraphBenchmark.jar -app=cc
    *
    * Options:
    *   -app "pagerank" , "cc" , "sssp" or "tc" for pagerank , connected components , sssp or triangleCount. (Default: pagerank)
    *   -niters the number of iterations of pagerank to use (Default: 10)
    *   -partStrategy the graph partitioning strategy to use
    *   -vifpath vertice input file path (Default: data/zw/vertice/part-*)
    *   -eifpath edge input file path (Default: data/zw/edge/part-*)
    *   -rofpath result output file path (Default: data/zw/${app}-result)
    */
  def main(args: Array[String]): Unit = {
    val options = args.map {
      arg =>
        arg.dropWhile(_ == '-').split('=') match {
          case Array(opt, v) => (opt -> v)
          case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
        }
    }

    var app = "pagerank"
    var niter = 10
    var partitionStrategy: Option[PartitionStrategy] = None
    var vifpath = "data/zw/vertice/part-*"
    var eifpath = "data/zw/edge/part-*"
    var rofpath = "data/zw/" + app + "-result"

    options.foreach {
      case ("app", v) => app = v
      case ("niters", v) => niter = v.toInt
      case ("partStrategy", v) => partitionStrategy = Some(PartitionStrategy.fromString(v))
      case ("vifpath", v) => vifpath = v
      case ("eifpath", v) => eifpath = v
      case ("rofpath", v) => rofpath = v
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }

    val conf = new SparkConf()
      .setAppName(s"GraphX Benchmark (app = $app)")
    GraphXUtils.registerKryoClasses(conf)

    val sc = new SparkContext(conf)

    val vertices:RDD[(VertexId,String)] = creVerticeRDD(sc, vifpath)

    val edges:RDD[Edge[String]] = creEdgeRDD(sc,eifpath)

    val unpartitionedGraph= Graph[String,String](vertices, edges)
    val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_)).cache()

    //Compute graph loading time
    var startTime = System.currentTimeMillis()
    val numEdges = graph.edges.count()
    println(s"Done creating graph. Num Edges = $numEdges")
    val loadTime = System.currentTimeMillis() - startTime
    println(s"Creation time = ${loadTime/1000.0} seconds")

    // Run app
    startTime = System.currentTimeMillis()
    runApp(app, graph, niter, rofpath)
    val runTime = System.currentTimeMillis() - startTime
    println(s"Run time = ${runTime/1000.0} seconds")

    sc.stop()
  }

  //Create RDD for all vertices
  def creVerticeRDD (sc: SparkContext, vifpath: String): RDD[(VertexId,String)] = {
    val vertices:RDD[(VertexId,String)] = sc.textFile(vifpath).map{ line =>
      val fields = line.substring(1, line.length-1).split(",")
      (fields(0).toLong, fields(1))
    }

    return vertices
  }

  //Create RDD for all edges
  def creEdgeRDD (sc: SparkContext, eifpath: String): RDD[Edge[String]] = {
    val edges:RDD[Edge[String]] = sc.textFile(eifpath).map{ line =>
      val fields = line.substring(5,line.length-1).split(",")
      Edge(fields(0).toLong, fields(1).toLong, fields(2))
    }

    return edges
  }

  //Run app
  def runApp (app: String, graph: Graph[String, String], niter: Int, rofpath: String) = {
    println(s"Running $app")
    if (app == "pagerank") {
      graph.staticPageRank(niter).vertices.sortByKey().saveAsTextFile(rofpath)
    } else if (app == "cc") {
      graph.connectedComponents.vertices.sortByKey().saveAsTextFile(rofpath)
    } else if (app == "sssp"){

      val sourceId: VertexId = 1 // The ultimate source
      // Initialize the graph such that all vertices except the root have distance infinity.
      val initialGraph = graph.mapEdges(e => e.attr.toDouble).mapVertices((id, _) =>
          if (id == sourceId) 0.0 else Double.PositiveInfinity)
      val sssp = initialGraph.pregel(Double.PositiveInfinity)(
        (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
        triplet => {  // Send Message
          if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
            Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
          } else {
            Iterator.empty
          }
        },
        (a, b) => math.min(a, b) // Merge Message
      )
      sssp.vertices.sortByKey().saveAsTextFile(rofpath)
    } else if (app == "tc"){
      graph.triangleCount().vertices.sortByKey().saveAsTextFile(rofpath)
    }
    println(s"Finish $app")
  }

}
