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
//      .where("domain = 'deepelectricalsupply.com'")

    val google = loadDataFrame("src/main/resources/google_dataset.csv")
      .as[Google]
      .filter(_.hasValidDomain) //0 not valid domains
//      .where("domain = 'deepelectricalsupply.com'")

    val website = loadDataFrame("src/main/resources/website_dataset.csv", Map("sep" -> ";"))
      .as[Website]
      .filter(_.hasValidDomain) //13 not valid domains
//      .where("root_domain = 'deepelectricalsupply.com'")

    //    analyzeDataset(facebook)
    //    analyzeDataset(google)
    //    analyzeDataset(website)

//    println(facebook.collect().head)
//    println(google.collect().head)
//    println(website.collect().head)


    val domainWindow = Window.partitionBy("domain").orderBy("name")
    val googleUniqueDomain = google
      .withColumn("row_number", row_number() over domainWindow)
      .filter(col("row_number") === 1)
      .drop(col("row_number"))
      .as[Google]

    val facebookGoogle = facebook
      .joinWith(googleUniqueDomain, facebook("domain") === googleUniqueDomain("domain"), "fullouter")
      .map(x => JoinedAll.from(Option(x._1), Option(x._2), None))
      .as[JoinedAll]

    val allJoinedOnDomain: Dataset[JoinedAll] = facebookGoogle
      .joinWith(website, facebookGoogle("domain") === website("root_domain"), "fullouter")
      .map(x => JoinedAll.from(x._1.f, x._1.g, Option(x._2)))
      .as[JoinedAll]

    allJoinedOnDomain
      .map(_.toFinalData)
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("src/main/resources/result_dataset")
  }
  process()
}
