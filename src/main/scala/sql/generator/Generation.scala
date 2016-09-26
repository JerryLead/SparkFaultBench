package sql.generator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import sql.generator.MyRandom

import collection.mutable.{ArrayBuffer, HashMap}
import scala.io.Source
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
  val path = "dataGenerated/sql/lcr/scripts/"
  //table

  val rankings_dict = Map(1->1000,2->10000,5->10000,10->10000)
  val uservisits_dict = Map(1->8000000,2->15000000,5->32000000,10->70000000)
  var rankings_col = 1000
  var uservisits_col = 5000000
  var scale = 10

  var urls = ArrayBuffer[String]()
  var urldict = new HashMap[String,Int]

  //zipf
  var num_bins = 0
  var zipf = ArrayBuffer[Int]()
  val zipf_param = 0.5

  val conf = new SparkConf()
    .setAppName("Gendata")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    //val scale = args(0).toInt
    //val zipf_params = args(1).toDouble
    rankings_col = rankings_dict(scale)
    uservisits_col = uservisits_dict(scale)


    load_zipf()
    genUrls()
    genOutputName()
    genRankingsFile(f1)
    genUservisitsFile(f2)
    GenType = "skewed"
    genOutputName()
    genRankingsFile(f1)
    genUservisitsFile(f2)
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
    return urls(zipf(no))
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

  def loadfile(filename:String): ArrayBuffer[String] ={

    var arr = ArrayBuffer[String]()
    val file = Source.fromFile("dataGenerated/sql/lcr/scripts/data_files/"+filename)
    for (line <- file.getLines()){
      arr+=line.trim()
    }
    return arr
  }


  def load_zipf(): Unit ={
    var numurls = rankings_col
    var sum = 0.0
    var min_bucket = 0
    var max_bucket = 0
    var residual = 0.0
    num_bins = rankings_col * 10

    for (i <- 1 to numurls){
      var value = 1.0 / math.pow(i, zipf_param)
      sum += value
    }
    for (i <- 0 to (numurls-1)) {
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

  def getDestinationUrl(): String ={
    if (GenType == "normal"){

      return urls(MyRandom.randomInt(0,urls.length-1))
    }
    else{

      val ra = MyRandom.randomBase()
      if (ra < 0.7)
        return urls(0)
      else
        return urls(MyRandom.randomInt(0,urls.length-1))
    }
  }

  def genRankingsFile(outputfile:String): Unit ={

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

  def genUservisitsFile(outputfile:String): Unit ={
    val agents = loadfile("user_agents.dat")
    val codes = loadfile("country_codes_plus_languages.dat")
    val keywords = loadfile("keywords.dat")
    //var pairs : Seq[RDD[(String,String,Int,Float,String,String,String,String,Int)]]=Seq()
    var pairs = ArrayBuffer[(String,String,Int,Float,String,String,String,String,Int)]()
    for (i <- 1 to uservisits_col){
      if (i % 100000 == 0)
        printf("%d\n",i)
      val sourceIP = genIP()
      val destURL = getDestinationUrl()
      val visitDate = MyRandom.randomDate()
      val adRevenue = MyRandom.randomFloat(1000.0f)
      val userAgent = agents(MyRandom.randomInt(0,agents.length-1)).replace(","," ").trim()
      val mycode = codes(MyRandom.randomInt(0,codes.length-1)).split(",")
      val countryCode = mycode(0).trim()
      val languageCode = mycode(1).trim()
      val searchWord = keywords(MyRandom.randomInt(0,(keywords.length-1)))
      val duration = MyRandom.randomInt(1,100)
      var pair = (sourceIP,destURL,visitDate,adRevenue,userAgent,countryCode,languageCode,searchWord,duration)

      pairs += pair
    }
    val rdd = sc.parallelize(pairs)
    rdd.saveAsTextFile(path+outputfile)
  }

  def genOutputName(): Unit ={
    f1 = "rankings"+"_"+GenType+"_"+scale+"G.txt"
    f2 = "uservisits"+"_"+GenType+"_"+scale+"G.txt"
  }

}
