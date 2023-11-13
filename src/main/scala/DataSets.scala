import JoinedAll.{calculateScore, pickCategory, pickCountyAndRegion, pickName, pickPhone}

import scala.util.matching.Regex

trait SourceDataset {
  private val domain_regexp: Regex = ("^(((?!-))(xn--|_)?[a-z0-9-]{0,61}[a-z0-9]{1,1}\\.)*(xn--)?([a-z0-9][a-z0-9\\-]{0,60}|[a-z0-9-]{1,30}\\.[a-z]{2,})$").r

  protected def validateDomain(domain: Option[String]): Boolean =
    domain.isDefined && domain_regexp.matches(domain.get)

  def hasValidDomain: Boolean
}

case class Facebook(domain: Option[String],
                    address: Option[String],
                    categories: Option[String],
                    city: Option[String],
                    country_code: Option[String],
                    country_name: Option[String],
                    description: Option[String],
                    email: Option[String],
                    link: Option[String],
                    name: Option[String],
                    page_type: Option[String],
                    phone: Option[String],
                    phone_country_code: Option[String],
                    region_code: Option[String],
                    region_name: Option[String],
                    zip_code: Option[String]
                   ) extends SourceDataset {
  override def hasValidDomain: Boolean = validateDomain(domain)
}

case class Google(
                   address: Option[String],
                   category: Option[String],
                   city: Option[String],
                   country_code: Option[String],
                   country_name: Option[String],
                   name: Option[String],
                   phone: Option[String],
                   phone_country_code: Option[String],
                   raw_address: Option[String],
                   raw_phone: Option[String],
                   region_code: Option[String],
                   region_name: Option[String],
                   text: Option[String],
                   zip_code: Option[String],
                   domain: Option[String]
                 ) extends SourceDataset {
  override def hasValidDomain: Boolean = validateDomain(domain)
}

case class Website(
                    root_domain: Option[String],
                    domain_suffix: Option[String],
                    language: Option[String],
                    legal_name: Option[String],
                    main_city: Option[String],
                    main_country: Option[String],
                    main_region: Option[String],
                    phone: Option[String],
                    site_name: Option[String],
                    tld: Option[String],
                    s_category: Option[String]
                  ) extends SourceDataset {
  override def hasValidDomain: Boolean = validateDomain(root_domain)
}

case class FinalData(
                      domain: String,
                      category: String,
                      country: String,
                      region: String,
                      phone: String,
                      name: String,
                      score: Int
                    )

case class JoinedAll(domain: String, f: Option[Facebook], g: Option[Google], w: Option[Website]) {
  def toFinalData: FinalData = {
    FinalData(
      domain,
      pickCategory(this),
      pickCountyAndRegion(this)._1,
      pickCountyAndRegion(this)._2,
      pickPhone(this),
      pickName(this),
      calculateScore(this)
    )
  }
}

object JoinedAll {
  def from(f: Option[Facebook], g: Option[Google], w: Option[Website]): JoinedAll = {
    //getting domain value from defined data
    val domain = (f, g, w) match {
      case (Some(f), _, _) => f.domain
      case (_, Some(g), _) => g.domain
      case (_, _, Some(w)) => w.root_domain
    }
    JoinedAll(domain.get, f, g, w)
  }

  /*
  For categories ranking method is used with most frequently used category from all sources
  For the Facebook source all categories split by | are taken to consideration
  */
  def pickCategory(input: JoinedAll): String = {
    val allCategories = extractCategories(input)

    //getting more frequent used category
    allCategories
      .map(_.trim)
      .filter(_.nonEmpty)
      .groupBy(identity)
      .view
      .mapValues(_.size)
      .iterator
      .maxByOption(_._2)
      .fold("")(_._1)
  }

  private def extractCategories(input: JoinedAll): List[String] = {
    val fCat = input.f.flatMap(f => f.categories).getOrElse("").split("\\|").toList
    val gCat = input.g.flatMap(g => g.category).getOrElse("")
    val wCat = input.w.flatMap(w => w.s_category).getOrElse("")

    (fCat ++ List(gCat) ++ List(wCat)).filter(_.nonEmpty)
  }

  /*
     For country and region we prefer Website next Google and as a last source Facebook
     They must be taken both from one source to be consistent
     */
  def pickCountyAndRegion(input: JoinedAll): (String, String) = {
    input.w
      .flatMap(w => w.main_country.flatMap(country => w.main_region.map(region => (country, region))))
      .orElse(input.g.flatMap(g => g.country_name.flatMap(country => g.region_name.map(region => (country, region)))))
      .orElse(input.f.map(f => (f.country_name.getOrElse(""), f.region_name.getOrElse(""))))
      .getOrElse(("", ""))
  }

  /*
   For phone we prefer Google source, next Facebook and the last Website
   */
  private def pickPhone(input: JoinedAll): String = {

    val gPhone = input.g.flatMap(_.phone)
    val fPhone = input.f.flatMap(_.phone)
    val wPhone = input.w.flatMap(_.phone)

    gPhone.orElse(fPhone).orElse(wPhone).getOrElse("")
  }


  private def extractNames(input: JoinedAll): List[Option[String]] =
    List(
      input.f.flatMap(_.name),
      input.g.flatMap(_.name),
      //For website_dataset if legal_name is empty we take site_name
      input.w.flatMap(w => if (w.legal_name.isDefined) Some(w.legal_name.get) else w.site_name)
    )

  /*
    For name we take the longest name amongst available sources
   */
  def pickName(input: JoinedAll): String = {
    val names = extractNames(input)

    //getting longest used name as the best
    names
      .flatten
      .map(el => (el, el.length))
      .maxByOption(_._2)
      .map(_._1)
      .getOrElse("")
  }

  // create from company name bag of words lower case
  // used for check if name are the same
  private def nameUnified(name: Option[String]): Set[String] = {
    //list of common world in name that can be removed
    val commonWords = List("ltd", "limited", "and", "inc", "incorporation", "company", "co", "gmbh")
    val words = name.map(_.split(" ")).toList

    words
      .flatten
      .map(_.replaceAll("\\W", ""))
      .map(_.trim)
      .map(_.toLowerCase)
      .filter(!commonWords.contains(_))
      .toSet
  }

  def calculateScore(input: JoinedAll): Int = {
    def namesSimilarity(): Int = {
      val names = extractNames(input)

      // +5 for defined name for each source, 2 or more are needed
      val nameScoreWithDefinedNames = if (names.flatten.size > 1) names.flatten.size * 5 else 0

      // +10 for the same names, 2 or more are needed
      val nameScoreWithSameNames = names
        .map(nameUnified)
        .filter(_.nonEmpty)
        .groupBy(identity)
        .view
        .mapValues(_.size)
        .iterator
        .maxByOption(_._2)
        .collect { case (_, count) if count > 1 => count * 10 }
        .getOrElse(0)

      nameScoreWithDefinedNames + nameScoreWithSameNames
    }

    def categorySimilarity(): Int = {
      val categories = extractCategories(input)

      // +10 for the same category name, 2 or more are needed
      categories
        .groupBy(identity)
        .view
        .mapValues(_.size)
        .iterator
        .maxByOption(_._2)
        .collect { case (_, count) if count > 1 => count * 10 }
        .getOrElse(0)
    }

    // +100 for each defined source
    val baseScore = Seq(input.f, input.g, input.w).count(_.isDefined) * 100
    val nameScore = namesSimilarity()
    val categoryScore = categorySimilarity()

    baseScore + nameScore + categoryScore
  }
}