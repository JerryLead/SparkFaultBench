package sql.generator

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, HashMap}


/**
  * Created by rui on 16-9-23.
  */
object Generation {
  //
  var GenType = "normal"

  //words
  val maxurllen = 100
  val minurllen = 10
  val maxwordlen = 10
  val minwordlen = 2
  val extractUrl = 30
  val link_fre = 0.05
  //file
  val filemean = 10000
  val filevar = 1000
  var f1 = "rankings.txt"
  var f2 = "uservisits.txt"
  val path = "hdfs:///user/hadoop/data/lcr/"
  //val path = "dataGenerated/sql/lcr/scripts/"
  //table

  val rankings_dict = Map(1->10,2->10000,5->10000,10->10000)
  val uservisits_dict = Map(1->8000000,2->15000000,5->32000000,10->70000000)
  var rankings_col = 1000
  var uservisits_col = 5000000
  var scale = 1
  var urls = ArrayBuffer[String]()
  var urldict = new HashMap[String,Int]

  //zipf
  var num_bins = 0
  var zipf_pagerank = ArrayBuffer[Int]()
  var zipf_param_pagerank = 0.5
  var zipf_url = ArrayBuffer[Int]()
  var zipf_param_url = 2.0


  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      scale = args(0).toInt
      if (scale>=10) scale = 10
      else if (scale >= 5) scale = 5
      else if (scale >= 2) scale = 2
      else scale = 1
    }
    if (args.length > 1) zipf_param_url = args(1).toFloat
    rankings_col = rankings_dict(scale)
    uservisits_col = uservisits_dict(scale)
    val conf = new SparkConf()
      .setAppName("Gensqldata")
    val sc = new SparkContext(conf)

    load_zipf(zipf_param_pagerank,zipf_pagerank)
    load_zipf(zipf_param_url,zipf_url)

    genUrls()
    genOutputName()
    genRankingsFileNoP(f1,sc)
    genUservisitsFile(f2,sc)
    GenType = "skewed"
    genOutputName()

    genRankingsFileNoP(f1,sc)
    genUservisitsFile(f2,sc)
    sc.stop()
  }

  def genUrls(): Unit ={
    val urlhead = "http://"
    val urllend = ".html"
    for (i <- 1 to rankings_col){
      val urllen = MyRandom.randomInt(minurllen,maxurllen)
      val urlname = MyRandom.randomUrlname(urllen)
      val strurl = urlhead+urlname+urllend
      urls += strurl
      if (i % 10000 == 0){
        printf ("generated %d urls\n", i)
      }
    }
    for (i <- 1 to rankings_col){
      genSimplePageContent()
      if (i % 1000 == 0){
        printf ("generated %d exctr url\n" , i)
      }
    }
  }

  def getUrl(): String ={
    val no = ((num_bins-1)*MyRandom.randomBase()).toInt
    return urls(zipf_url(no))
  }

  def genSimplePageContent(): Unit ={
    for (i <- 1 to extractUrl){
      var extUrl = getUrl()
      if (urldict.contains(extUrl)){
        urldict(extUrl) += 1
      }
      else {
        urldict(extUrl) = 1
      }
    }
  }
  def genPageContent(): Unit ={
    var totallen = 0
    val filelen = (filemean + filevar * MyRandom.randomNormal()).toInt
    while (totallen>=filelen){
      val fl = MyRandom.randomBase()
      var contentlen = 0
      if (fl < link_fre){
        val extUrl = getUrl()
        if (urldict.contains(extUrl)){
          urldict(extUrl) += 1
        }
        else {
          urldict(extUrl) = 1
        }
        contentlen = extUrl.length()
      }
      else{
        contentlen = MyRandom.randomInt(minwordlen,maxwordlen)
      }
      totallen += contentlen
    }

  }
  def genIP(): String ={
    val fst = MyRandom.randomInt(0,223)
    val snd = MyRandom.randomInt(0,255)
    val trd = MyRandom.randomInt(0,255)
    val fth = MyRandom.randomInt(0,255)
    return fst.toString+'.'+snd.toString+'.'+trd.toString+'.'+fth.toString
  }


  def load_zipf(zipf_param:Double,zipf:ArrayBuffer[Int]): Unit ={
    var numurls = rankings_col
    var sum = 0.0
    var min_bucket = 0
    var max_bucket = 0
    var residual = 0.0
    num_bins = rankings_col * 10

    for (i <- 1 until numurls){
      var value = 1.0 / math.pow(i, zipf_param)
      sum += value
    }
    for (i <- 0 until (numurls-1)) {
      val link_prob = (1.0 / math.pow((i + 1), zipf_param)) / sum
      max_bucket = (min_bucket + num_bins * (link_prob + residual)).toInt
      for (j <- min_bucket to max_bucket-1) {
        zipf += i
      }
      residual += link_prob - ((max_bucket - min_bucket).toFloat / num_bins)
      if (residual < 0) {
        residual = 0
        min_bucket = max_bucket
      }
    }
  }

  def getDestinationUrl(urls:ArrayBuffer[String],zipf:ArrayBuffer[Int]): String ={
    if (GenType == "normal"){
      return urls(MyRandom.randomInt(0,urls.length-1))
    }
    else{
      val no = ((zipf.length-1)*MyRandom.randomBase()).toInt
      return urls(zipf(no))
    }
  }

  def genRankingsFile(outputfile:String,sc:SparkContext): Unit ={

    val rdd = sc.parallelize(0 to rankings_col-1, 8).map(
      x => {
        val pageurl = urls(x)
        var pagerank = 0
        if (urldict.contains(pageurl))
          pagerank = urldict(pageurl)
        else
          pagerank = 1
        val avgDuration = MyRandom.randomInt(1,100)
        (pagerank,pageurl,avgDuration)
      }
    )
    rdd.saveAsTextFile(path+outputfile)
  }
  def genRankingsFileNoP(outputfile:String,sc:SparkContext): Unit ={

    var pairs = ArrayBuffer[(Int,String,Int)]()
    for (pageurl <- urls){
      var pagerank = 0
      if (urldict.contains(pageurl))
        pagerank = urldict(pageurl)
      else
        pagerank = 1
      val avgDuration = MyRandom.randomInt(1,100)
      var pair = (pagerank,pageurl,avgDuration)

      pairs += pair
    }
    val rdd = sc.parallelize(pairs)
    rdd.saveAsTextFile(path+outputfile)
  }


  def genUservisitsFile(outputfile:String,sc:SparkContext): Unit ={
    val broadcast_agents = sc.broadcast(DataFile.agents)
    val broadcast_codes = sc.broadcast(DataFile.codes)
    val broadcast_keywords = sc.broadcast(DataFile.keywords)
    val broadurls = sc.broadcast(urls)
    val broadzipf = sc.broadcast(zipf_url)
    val rdd = sc.parallelize(1 to uservisits_col, 8).map(
      x => {
        val sourceIP = genIP()
        val destURL = getDestinationUrl(broadurls.value,broadzipf.value)
        val visitDate = MyRandom.randomDate()
        val adRevenue = MyRandom.randomFloat(1000.0f)
        val userAgent = broadcast_agents.value(MyRandom.randomInt(0,broadcast_agents.value.length-1)).replace(","," ").trim()
        val mycode = broadcast_codes.value(MyRandom.randomInt(0,broadcast_codes.value.length-1)).split(",")
        val countryCode = mycode(0).trim()
        val languageCode = mycode(1).trim()
        val searchWord = broadcast_keywords.value(MyRandom.randomInt(0,(broadcast_keywords.value.length-1)))
        val duration = MyRandom.randomInt(1,100)
        (sourceIP,destURL,visitDate,adRevenue,userAgent,countryCode,languageCode,searchWord,duration)
      }
    )
    rdd.saveAsTextFile(path+outputfile)

  }

  def genOutputName(): Unit ={
    f1 = "rankings"+"_"+GenType+"_"+scale+"G"
    f2 = "uservisits"+"_"+GenType+"_"+scale+"G"
  }

}
