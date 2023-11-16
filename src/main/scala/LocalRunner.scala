import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object LocalRunner extends App {
  private def analyzeDataset[A](dataset: Dataset[A]): Unit = {
    dataset.describe(dataset.columns: _*).show()
    dataset.select(dataset.columns.map(c => countDistinct(col(c)).alias(s"unq($c)")): _*).show()
    println(s"count: ${dataset.count()}")
    dataset.show()
  }

  private val spark = SparkSession
    .builder()
    .master("local[4]")
    .appName("local")
    .getOrCreate()

  import spark.implicits._

  private def loadDataFrame(location: String, additionalOptions: Map[String, String] = Map()): DataFrame = {
    spark
      .read
      .option("header", "true")
      .options(additionalOptions)
      .csv(location)
  }

  private def process(): Unit = {
    val facebook = loadDataFrame("src/main/resources/facebook_dataset.csv", Map("multiline" -> "true"))
      .as[Facebook]
      .filter(_.hasValidDomain) //0 not valid domains

    val google = loadDataFrame("src/main/resources/google_dataset.csv")
      .as[Google]
      .filter(_.hasValidDomain) //0 not valid domains

    val website = loadDataFrame("src/main/resources/website_dataset.csv", Map("sep" -> ";"))
      .as[Website]
      .filter(_.hasValidDomain) //13 not valid domains - empty data perceived as garbage

    //    analyzeDataset(facebook) //all domains are unique
    //    analyzeDataset(google)  //a lot of not unique domains
    //    analyzeDataset(website) //all domains are unique

    val domainWindow = Window.partitionBy("domain").orderBy("name")

    // For joining from Google we take only records with unique domain
    val googleUniqueDomain = google
      .withColumn("domainCount", count("domain") over domainWindow)
      .filter(col("domainCount") === 1)
      .drop(col("domainCount"))
      .as[Google]

    // Records with not unique domains will be later unionAll for joined data
    // It was considered to identify companies that have many subsidiary with the same domain name
    // from the others like common domain like facebook.com, but it could only help to improve accuracy for
    // categories (not for phones, country, region and probably name)
    val googleNotUniqueDomain = google
      .withColumn("domainCount", count("domain") over domainWindow)
      .filter(col("domainCount") > 1)
      .drop(col("domainCount"))
      .as[Google]

    // First join facebook (we have all unique domains) with only google unique domains
    val facebookGoogle = facebook
      .joinWith(googleUniqueDomain, facebook("domain") === googleUniqueDomain("domain"), "fullouter")
      .map(x => JoinedAll.from(Option(x._1), Option(x._2), None))
      .as[JoinedAll]

    // Second join previous result with website data (we have all unique domains)
    val allJoinedOnDomain: Dataset[JoinedAll] = facebookGoogle
      .joinWith(website, facebookGoogle("domain") === website("root_domain"), "fullouter")
      .map(x => JoinedAll.from(x._1.f, x._1.g, Option(x._2)))
      .as[JoinedAll]

    // At the end we unionAll google non unique domains for joined data
    val all = allJoinedOnDomain
      .unionAll(
        googleNotUniqueDomain
        .map(x => JoinedAll.from(None, Option(x), None))
        .as[JoinedAll]
      )

    all
      .map(_.toFinalData) // most of the processing is done here
      .as[FinalData]
      .orderBy(col("score").desc)
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("src/main/resources/result_dataset")
  }
  process()
}
