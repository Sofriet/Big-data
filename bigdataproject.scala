package org.rubigdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import collection.JavaConverters._
import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat, WarcWritable}
import scala.util.matching.Regex
import java.net.URL

object RUBigDataApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("RUBigDataApp").getOrCreate()
    import spark.implicits._

    val twentyThreeFiles = "hdfs:///cc-crawl/segments/1618038072082.26/warc/CC-MAIN-20210413031741-20210413061741-00187.warc.gz," +
                      "hdfs:///cc-crawl/segments/1618039388763.75/warc/CC-MAIN-20210420091336-20210420121336-00188.warc.gz," +
                      "hdfs:///cc-crawl/segments/1618039388763.75/warc/CC-MAIN-20210420091336-20210420121336-00039.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00002.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00003.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00004.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00005.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00006.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00007.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00008.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00009.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00010.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00011.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00012.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00013.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00014.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00015.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00016.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00017.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00018.warc.gz," +
                      "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00019.warc.gz"

    //val segmentDir = "hdfs:///cc-crawl/segments/1618038067400.24/warc"
    //val hadoopConf = spark.sparkContext.hadoopConfiguration
    //val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //val fileStatus = fs.listStatus(new org.apache.hadoop.fs.Path(segmentDir))
    //val warcFiles = fileStatus.filter(_.getPath.getName.endsWith(".warc.gz")).map(_.getPath.toString).mkString(",")

    val sc = spark.sparkContext
    val warcs = sc.newAPIHadoopFile(
      twentyThreeFiles,
      classOf[WarcGzInputFormat],
      classOf[NullWritable],
      classOf[WarcWritable]
    )

    val validWarcs = warcs.map{wr => wr._2}.filter(_.isValid())

    val sunfilterRegex = "(?i)(oxybenzone|octinoxate|homosalate)".r
    val parfumRegex = "(?i)(parfum|fragrance)".r
    val pigmentRegex = "(?i)hydroquinone".r
    val alcoholRegex = "(?i)alcohol".r
    val parabenRegex = "(?i)paraben".r
    val combinedRegex = "(?i)(oxybenzone|octinoxate|homosalate|parfum|fragrance|hydroquinone|alcohol|paraben)".r

    def hasAtLeastTwoMatches(text: String, regex: Regex): Boolean = {
      var count = 0
      val matcher = regex.pattern.matcher(text)
      while (matcher.find() && count < 2) {
        count += 1
      }
      count >= 2
    }

    def extractDomain(url: String): String = {
       try {
         new URL(url).getHost.replace("www.", "")
       } catch {
         case _: Exception => url
       }
    }

    val result = validWarcs.map(_.getRecord()).filter(_.isHttp())
      .filter { rec =>
        val url = rec.getHeader.getUrl
        val body = rec.getHttpStringBody
        val content = if (body != null) Jsoup.parse(body).body() else null
        val text = if (content != null) content.text() else null
        url != null && url.nonEmpty && text != null //&& hasAtLeastTwoMatches(body, combinedRegex)
      }
      .map { rec =>
        val domain = extractDomain(rec.getHeader.getUrl)
        val text = rec.getHttpStringBody()
        val sunfilterCount = sunfilterRegex.findAllIn(text).length
        val parfumCount = parfumRegex.findAllIn(text).length
        val pigmentCount = pigmentRegex.findAllIn(text).length
        val alcoholCount = alcoholRegex.findAllIn(text).length
        val parabenCount = parabenRegex.findAllIn(text).length
        (domain, alcoholCount, parabenCount, parfumCount, pigmentCount, sunfilterCount)
      }.toDF("Domain", "Alcohol Count", "Paraben Count", "Parfum Count", "Pigment Count", "Sunfilter Count")
       .groupBy("Domain")
       .sum("Alcohol Count", "Paraben Count", "Parfum Count", "Pigment Count", "Sunfilter Count")
      //.collect()

    // Print the results in CSV format
    //println("URL,Alcohol Count,Paraben Count,Parfum Count,Pigment Count,Sunfilter Count")
    //result.foreach { case (url, alcoholCount, parabenCount, parfumCount, pigmentCount, sunfilterCount) =>
     // println(s"$url,$alcoholCount,$parabenCount,$parfumCount,$pigmentCount,$sunfilterCount")
    //}

    // Uncomment to write the result to CSV
    // val resultDF = result.toDF("URL", "Alcohol Count", "Paraben Count", "Parfum Count", "Pigment Count", "Sunfilter Count")
     result.write.option("header", "true").csv("hdfs:///user/s1068747/rubigdata-segment.csv")

    spark.stop()
  }
}

