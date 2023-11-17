import JoinedAll.{RecordScore, mergeCategory, mergeCountyAndRegion, mergeName, mergePhone}

/*
  Container for joined records from Facebook, Google and Website sources used to joins and union data
 */
case class JoinedAll(domain: String, f: Option[Facebook], g: Option[Google], w: Option[Website]) {

  // Create a final output data for current record
  def toFinalData: FinalData = {

    //we get merged values and calculate the score as well
    val (name, nameScore) = mergeName(this)
    val (category, categoryScore) = mergeCategory(this)
    val (phone, phoneScore) = mergePhone(this)
    val (countryRegion, countryRegionScore) = mergeCountyAndRegion(this)


    val sourceDefined = Seq(this.f, this.g, this.w).count(_.isDefined)
    val recordScore = RecordScore(sourceDefined, nameScore, categoryScore, phoneScore, countryRegionScore)

    FinalData(domain = domain,
      name = name,
      category = category,
      phone = phone,
      country = countryRegion._1,
      region = countryRegion._2,
      score = recordScore.score
    )
  }
}

/*
  Companion object used to encapsulate most of the processing logic
 */
object JoinedAll {

  /*
      Helper class used to describe quality of JoinedAll data for Facebook, Google and Website sources.
      Contains information
        * how many data sources are available
        * how many values are defined for name, category, phone and countryRegion
        * how many values defined for name, category, phone and countryRegion are perceived as the same - it depends on the type
        of data, e.g. for company name to compare is used lowecase bag of words with removed common words
        * we get also list of similar values (perceived as the same)

      This helper data are used later when selecting final result for joined data
     */
  case class RecordScore(sourceDefined: Int, name: Similarity[String],
                         category: Similarity[String],
                         phone: Similarity[String],
                         countryRegion: Similarity[(String, String)]) {


    // implementation of score value for joined data for record
    // this score defines the quality of final data
    // + 10 for each defined source - possible values 30, 20, 10
    // + 2 for the similar values for name, category, phone and countryRegion - possible values 4 (two similar), 6 (three similar) or 0 otherwise
    // + 1 for each defined data for name, category, phone and countryRegion - possible value 0, 1, 2, 3
    def score: Int =
      sourceDefined * 10 +
        name.defined + name.similarity * 2 +
        category.defined + category.similarity * 2 +
        phone.defined + phone.similarity * 2 +
        countryRegion.defined + countryRegion.similarity * 2
  }

  // just simple wrapper for defined and similar score with list of similar value - considered the same
  case class Similarity[A](defined: Int, similarity: Int, similarList: List[A])

  /*
    helper method that is used handle joins of datasets
   */
  def from(f: Option[Facebook], g: Option[Google], w: Option[Website]): JoinedAll = {
    //getting domain value from defined data, first for defined is taken
    val domain = (f, g, w) match {
      case (Some(f), _, _) => f.domain
      case (_, Some(g), _) => g.domain
      case (_, _, Some(w)) => w.root_domain
    }
    JoinedAll(domain.get, f, g, w)
  }

  /*
    Generic method for merging values basing on Similarity score - logic described in method
   */
  private def mergeGeneric[A](record: JoinedAll, allValues: List[Option[A]], score: Similarity[A], empty: A,
                              getArbitraryValue: JoinedAll => A, getValueFromSimilar: List[A] => A): A =
    score match {
      // we don't have similar values and only one defined source - we just take it as a result
      case Similarity(defined, _, Nil) if defined == 1 => allValues.flatten.head

      // we don't have similar values and at least two source defined - this is conflict -
      // in that case we apply arbitrary mapping function for this record
      // as alternative solution can be used splitting up this record
      case Similarity(defined, _, Nil) if defined > 1 => getArbitraryValue(record)

      // we don't have similar values and not defined values - this is trash data return empty value
      case Similarity(defined, _, Nil) if defined == 0 => empty

      // we hava at least two similar values - in that case we use function to get from similar list
      // as alternative approach we can only use this case when defined == similar and split up record otherwise
      case Similarity(_, _, similar: List[A]) => getValueFromSimilar(similar)
    }

  /*
    Generic method for calculating score
   */
  private def scoreGeneric[A](values: List[Option[A]], nonEmpty: A => Boolean,
                              getSimilar: List[Option[A]] => Option[List[A]]): Similarity[A] = {

    // +1 for each non empty value
    val defined = values.flatten.count(nonEmpty)

    //get similar values, at leas 2 needed
    val similar: List[A] = getSimilar(values)
      .collect { case list if list.size > 1 => list }
      .getOrElse(List[A]())

    Similarity(defined, similar.size, similar)
  }

  //common method to set value for similar is this case the longest value is considered the best
  private def getLongestFromSimilar(similar: List[String]): String = similar
    .map(el => (el, el.length))
    .maxByOption(_._2)
    .map(_._1)
    .getOrElse("")

  /*
    Company names merging logic described in method body
   */
  def mergeName(input: JoinedAll): (String, Similarity[String]) = {

    /*
      method creates from company names bag of words lower case
      from word are removed any non characters and trimmed
      also some common words are removed
      used later to check if company names are the same
     */
    def nameUnified(name: String): Set[String] = {

      //list of common world in names that can be removed - list should be extended (different languages support etc)
      val commonWords = List("ltd", "limited", "and", "inc", "incorporation", "company", "co", "gmbh")
      val words = name.split(" ").toList

      words
        .map(_.replaceAll("\\W", "")) //remove any not chars
        .map(_.trim)
        .map(_.toLowerCase)
        .filter(!commonWords.contains(_))
        .toSet
    }

    // to get similar company name we use sanitize function nameUnified and used result bag of words to compate
    def getSimilarNames(names: List[Option[String]]): Option[List[String]] = names
      .flatten
      .map(el => nameUnified(el) -> el) //bag of words used to represent name
      .filter(_._1.nonEmpty)
      .groupMap(_._1)(_._2)
      .iterator
      .maxByOption(_._2.size)
      .map(_._2)

    // decision what name should be taken as final one
    // for company names we get data in particular order Facebook, Google, Website
    def namesArbitraryMapping(input: JoinedAll): String = {
      val (fValue, gValue, wValue) = extractNames(input)
      fValue.orElse(gValue).orElse(wValue).getOrElse("")
    }

    val (fValue, gValue, wValue) = extractNames(input)
    val namesAll = List(fValue, gValue, wValue)
    val score = scoreGeneric(namesAll, (el: String) => el.nonEmpty, getSimilarNames)
    val name = mergeGeneric(input, namesAll, score, "", namesArbitraryMapping, getLongestFromSimilar)

    (name, score)
  }


  /*
      Mering category logic described in method body
    */
  def mergeCategory(input: JoinedAll): (String, Similarity[String]) = {

    // to get similar categories we only sanitize using trim and tolowercase
    def getSimilarCategories(categories: List[Option[String]]): Option[List[String]] = categories
      .flatten
      .flatMap(_.split("\\|").toSet.toList) // extracting facebook categories only unique values
      .map(el => el.toLowerCase().trim -> el)
      .filter(_._1.nonEmpty)
      .groupMap(_._1)(_._2)
      .iterator
      .maxByOption(_._2.size)
      .map(_._2)

    // decision what category should be taken as final one
    // for categories we get data in particular order Facebook, Google, Website
    def categoryArbitraryMapping(input: JoinedAll): String = {
      val (fValue, gValue, wValue) = extractCategories(input)
      fValue.orElse(gValue).orElse(wValue).getOrElse("")
    }

    val (fValue, gValue, wValue) = extractCategories(input)
    val allCategories = List(fValue, gValue, wValue)
    val score = scoreGeneric(allCategories, (el: String) => el.nonEmpty, getSimilarCategories)
    val category = mergeGeneric(input, allCategories, score, "", categoryArbitraryMapping, getLongestFromSimilar)

    (category, score)
  }

  /*
   Phone merging logic described in method body
   */
  def mergePhone(input: JoinedAll): (String, Similarity[String]) = {

    // to get similar phones we sanitize removing all not number values
    def getSimilarPhones(phones: List[Option[String]]): Option[List[String]] =
      phones
        .flatten
        .map(el => el.replaceAll("\\D", "").trim -> el)
        .filter(_._1.nonEmpty)
        .groupMap(_._1)(_._2)
        .iterator
        .maxByOption(_._2.size)
        .map(_._2)

    // decision what category should be taken as final one
    // for categories we get data in particular order Facebook, Google, Website
    def categoryArbitraryMapping(input: JoinedAll): String = {
      val (gPhone, fPhone, wPhone) = extractPhones(input)
      gPhone.orElse(fPhone).orElse(wPhone).getOrElse("")
    }

    val (gPhone, fPhone, wPhone) = extractPhones(input)
    val score = scoreGeneric(List(gPhone, fPhone, wPhone), (el: String) => el.nonEmpty, getSimilarPhones)
    val phone = mergeGeneric(input, List(gPhone, fPhone, wPhone), score, "", categoryArbitraryMapping, getLongestFromSimilar)

    (phone, score)
  }

  /*
     For country and region we prefer Website next Google and as a last source Facebook
     They must be taken both from one source to be consistent
     */
  def mergeCountyAndRegion(input: JoinedAll): ((String, String), Similarity[(String, String)]) = {

    // to get similar categories we only sanitize using tolowecase and trim
    def getSimilarCountryRegion(countryRegions: List[Option[(String, String)]]): Option[List[(String, String)]] =
      countryRegions
        .flatten
        .map(el => (el._1.toLowerCase().trim, el._1.toLowerCase().trim) -> (el._1, el._2))
        .filter(el => el._1._1.nonEmpty && el._1._2.nonEmpty)
        .groupMap(_._1)(_._2)
        .iterator
        .maxByOption(_._2.size)
        .map(_._2)

    // decision what country region should be taken as final one
    // we get data in particular order Facebook, Google, Website
    def countryRegionArbitraryMapping(input: JoinedAll): (String, String) = {
      val (wCountryRegion, gCountryRegion, fCountryRegion) = extractCountryRegion(input)
      fCountryRegion.orElse(gCountryRegion).orElse(wCountryRegion).getOrElse(("", ""))
    }

    //from the similar we just take first
    def getCountryRegionFromSimilar(similar: List[(String, String)]): (String, String) = similar.head

    val (wCountryRegion, gCountryRegion, fCountryRegion) = extractCountryRegion(input)
    val countryRegions = List(wCountryRegion, gCountryRegion, fCountryRegion)
    val score = scoreGeneric(countryRegions, (el: (String, String)) => el._1.nonEmpty && el._2.nonEmpty, getSimilarCountryRegion)
    val countryRegion = mergeGeneric(input, countryRegions, score, ("", ""), countryRegionArbitraryMapping, getCountryRegionFromSimilar)

    (countryRegion, score)
  }

  private def extractPhones(input: JoinedAll): (Option[String], Option[String], Option[String]) = (
    input.g.flatMap(_.phone),
    input.f.flatMap(_.phone),
    input.w.flatMap(_.phone)
  )

  private def extractNames(input: JoinedAll): (Option[String], Option[String], Option[String]) = (
    input.f.flatMap(_.name),
    input.g.flatMap(_.name),
    //For website_dataset if legal_name is empty we take site_name
    input.w.flatMap(w => if (w.legal_name.isDefined && w.legal_name.get.nonEmpty) Some(w.legal_name.get) else w.site_name)
  )

  private def extractCategories(input: JoinedAll): (Option[String], Option[String], Option[String]) = (
    input.f.flatMap(f => f.categories),
    input.g.flatMap(g => g.category),
    input.w.flatMap(w => w.s_category)
  )

  private def extractCountryRegion(input: JoinedAll): (Option[(String, String)], Option[(String, String)], Option[(String, String)]) = (
    input.w.flatMap(w => w.main_country.flatMap(country => w.main_region.map(region => (country, region)))),
    input.g.flatMap(g => g.country_name.flatMap(country => g.region_name.map(region => (country, region)))),
    input.f.flatMap(f => f.country_name.flatMap(country => f.region_name.map(region => (country, region))))
  )
}
