import JoinedAll.{RecordScore, mergeCategory, mergeCountyAndRegion, mergeName, mergePhone}

/*
  Container for joined records from Facebook, Google and Website sources used to joins and union data
 */
case class JoinedAll(domain: String, f: Option[Facebook], g: Option[Google], w: Option[Website]) {

  // Create a final output data for current record
  def toFinalData: FinalData = {

    //we get merged values as well the score for this value
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

    /*
      Calculates as
        * definedScore as + 1 for each non empty name
        * similarityScore as +1 each the same names (at least two needed) - cleaned bag of work compared
        * return List of names that are considered the same - using namesUnified function
     */
    def namesScore(names: List[Option[String]]): Similarity[String] = {

      // +1 for each non empty names
      val definedScore = names
        .flatten
        .count(_.nonEmpty)

      // +1 for the same names, 2 or more are needed
      // for names there is used nameUnifies - bag of words representation for names
      val similarNames = names
        .flatten
        .map(el => nameUnified(el) -> el) //bag of words used to represent name
        .filter(_._1.nonEmpty)
        .groupMap(_._1)(_._2)
        .iterator
        .maxByOption(_._2.size)
        .collect { case (_, list) if list.size > 1 => list }
        .getOrElse(List())

      Similarity(definedScore, similarNames.size, similarNames)
    }

    val namesAll = extractNames(input)
    val score = namesScore(namesAll)

    // decision what name should be taken as final one
    val name = score match {
      // we don't have similar values and only one defined source - we just take it as a result
      case Similarity(defined, _, Nil) if defined == 1 =>
        namesAll
          .flatten
          .head

      // we don't have similar values and at least two source defined - this is conflict -
      // in that case we get data in name in particular order Facebook, Google, Website
      // as alternative solution can be used splitting up this record
      case Similarity(defined, _, Nil) if defined > 1 =>
        val fValue = input.f.flatMap(_.name)
        val gValue = input.g.flatMap(_.name)
        val wValue = input.w.flatMap(_.legal_name).orElse(input.w.flatMap(_.site_name))

        fValue.orElse(gValue).orElse(wValue).getOrElse("")

      // we don't have similar values and not defined values - this is trash data return empty string
      case Similarity(defined, _, Nil) if defined == 0 => ""

      // we hava at least two similar values - in that case we just pick the longest one fro the similar ones
      // as alternative approach we can only use this case when defined == similar and split up record otherwise
      case Similarity(_, _, similar: List[String]) =>
        similar
          .map(el => (el, el.length))
          .maxByOption(_._2)
          .map(_._1)
          .getOrElse("")
    }

    (name, score)
  }

  /*
    Mering category logic described in method body
  */
  def mergeCategory(input: JoinedAll): (String, Similarity[String]) = {
    /*
      Calculates as
        * definedScore as + 1 for each non empty category
        * similarityScore as +1 each the same category (at least two needed) - cleaned values compared
     */
    def categoryScore(categories: List[String]): Similarity[String] = {

      // +1 for each non empty category
      val definedScore = categories
        .count(_.nonEmpty)
        .min(3) // restricting to 3 defined (to avoid impact of many categories for Facebook)

      // +1 for the same category names, 2 or more are needed
      val similarCategories = categories
        .map(el => el.toLowerCase().trim -> el)
        .filter(_._1.nonEmpty)
        .groupMap(_._1)(_._2)
        .iterator
        .maxByOption(_._2.size)
        .collect { case (_, list) if list.size > 1 => list }
        .getOrElse(List())

      Similarity(definedScore, similarCategories.size, similarCategories)
    }

    val allCategories = extractCategories(input)
    val score = categoryScore(allCategories)

    val category = score match {
      // we don't have similar values and only one defined source - we just take it as a result
      case Similarity(defined, _, Nil) if defined == 1 => allCategories.head

      // we don't have similar values and at least two source defined - this is conflict -
      // in that case we get data in name in particular order Facebook, Google, Website
      // as alternative solution can be used splitting up this record
      case Similarity(defined, _, Nil) if defined > 1 =>
        val fValue = input.f.flatMap(_.categories)
        val gValue = input.g.flatMap(_.category)
        val wValue = input.w.flatMap(_.s_category)

        fValue.orElse(gValue).orElse(wValue).getOrElse("")

      // we don't have similar values and not defined values - this is trash data return empty string
      case Similarity(defined, _, Nil) if defined == 0 => ""

      // we hava at least two similar values - in that case we just pick the longest one from the similar ones
      // as alternative approach we can only use this case when defined == similar and split up record otherwise
      case Similarity(_, _, similar: List[String]) =>
        similar
          .map(el => (el, el.length))
          .maxByOption(_._2)
          .map(_._1)
          .getOrElse("")
    }

    (category, score)
  }

  /*
   Phone merging logic described in method body
   */
  def mergePhone(input: JoinedAll): (String, Similarity[String]) = {

    /*
      Calculates as
        * definedScore as + 1 for each non empty phone
        * similarityScore as +1 each the same phone number (at least two needed) - cleaned values compared
     */
    def phoneScore(): Similarity[String] = {
      val (gPhone, fPhone, wPhone) = extractPhones(input)
      val phones = List(gPhone, fPhone, wPhone)

      val definedScore = phones
        .flatten
        .count(_.nonEmpty)

      // +1 for the same phone, 2 or more are needed
      // all not digit chars removed from phone
      val similarPhones = phones
        .flatten
        .map(el => el.replaceAll("\\D", "").trim -> el)
        .filter(_._1.nonEmpty)
        .groupMap(_._1)(_._2)
        .iterator
        .maxByOption(_._2.size)
        .collect { case (_, list) if list.size > 1 => list }
        .getOrElse(List())

      Similarity(definedScore, similarPhones.size, similarPhones)
    }

    val (gPhone, fPhone, wPhone) = extractPhones(input)

    val score = phoneScore()

    val phone = score match {
      // we don't have similar values and only one defined source - we just take it as a result
      case Similarity(defined, _, Nil) if defined == 1 => gPhone.orElse(fPhone).orElse(wPhone).getOrElse("") //only one is not empty

      // we don't have similar values and at least two source defined - this is conflict -
      // in that case we get data in name in particular order Facebook, Google, Website
      // as alternative solution can be used splitting up this record
      case Similarity(defined, _, Nil) if defined > 1 => gPhone.orElse(fPhone).orElse(wPhone).getOrElse("")

      // we don't have similar values and not defined values - this is trash data return empty string
      case Similarity(defined, _, Nil) if defined == 0 => ""

      // we hava at least two similar values - in that case we just pick the longest one from the similar ones
      // as alternative approach we can only use this case when defined == similar and split up record otherwise
      case Similarity(_, _, similar: List[String]) =>
        similar
          .map(el => (el, el.length))
          .maxByOption(_._2)
          .map(_._1)
          .getOrElse("")
    }
    (phone, score)
  }


  /*
     For country and region we prefer Website next Google and as a last source Facebook
     They must be taken both from one source to be consistent
     */
  def mergeCountyAndRegion(input: JoinedAll): ((String, String), Similarity[(String, String)]) = {
    /*
      Calculates as
        * definedScore as + 1 for each non empty pair for country and region
        * similarityScore as +1 each the same pair (at least two needed)
     */
    def countryAdnRegionScore():Similarity[(String, String)] = {
      val (wCountryRegion, gCountryRegion, fCountryRegion) = extractCountryRegion(input)
      val countryRegions = List(wCountryRegion, gCountryRegion, fCountryRegion)

      // +1 for each defined pair of country and region
      val definedScore = countryRegions
        .flatten
        .count(x => x._1.nonEmpty && x._2.nonEmpty)

      // +1 for the country and region, 2 or more are needed
      val similarList = countryRegions
        .flatten
        .map(el => (el._1.toLowerCase().trim, el._1.toLowerCase().trim) -> (el._1, el._2)) // we can think about using bag of words also here
        .filter(el => el._1._1.nonEmpty && el._1._2.nonEmpty)
        .groupMap(_._1)(_._2)
        .iterator
        .maxByOption(_._2.size)
        .collect { case (_, list) if list.size > 1 => list }
        .getOrElse(List())


      Similarity(definedScore, similarList.size, similarList)
    }

    val (wCountryRegion, gCountryRegion, fCountryRegion) = extractCountryRegion(input)
    val score = countryAdnRegionScore()

    val countryRegion = score match {
      // we don't have similar values and only one defined source - we just take it as a result
      case Similarity(defined, _, Nil) if defined == 1 =>
        fCountryRegion.orElse(gCountryRegion).orElse(wCountryRegion).getOrElse(("", "")) //only one is not empty

      // we don't have similar values and at least two source defined - this is conflict -
      // in that case we get data in name in particular order Facebook, Google, Website
      // as alternative solution can be used splitting up this record
      case Similarity(defined, _, Nil) if defined > 1 =>

        fCountryRegion.orElse(gCountryRegion).orElse(wCountryRegion).getOrElse(("", ""))

      // we don't have similar values and not defined values - this is trash data return empty string
      case Similarity(defined, _, Nil) if defined == 0 => ("", "")

      // we hava at least two similar values - in that case we just pick the longest one from the similar ones
      // as alternative approach we can only use this case when defined == similar and split up record otherwise
      case Similarity(_, _, similar: List[(String, String)]) =>
        similar.head
    }

    (countryRegion, score)
  }

  private def extractPhones(input: JoinedAll): (Option[String], Option[String], Option[String]) =
    (input.g.flatMap(_.phone),
      input.f.flatMap(_.phone),
      input.w.flatMap(_.phone))

  private def extractNames(input: JoinedAll): List[Option[String]] =
    List(
      input.f.flatMap(_.name),
      input.g.flatMap(_.name),
      //For website_dataset if legal_name is empty we take site_name
      input.w.flatMap(w => if (w.legal_name.isDefined && w.legal_name.get.nonEmpty) Some(w.legal_name.get) else w.site_name)
    )

  private def extractCategories(input: JoinedAll): List[String] = {
    //we need to drop not unique categories for facebook data
    val fCat = input.f.flatMap(f => f.categories).getOrElse("").split("\\|").toSet.toList //only unique names are taken
    val gCat = input.g.flatMap(g => g.category).getOrElse("")
    val wCat = input.w.flatMap(w => w.s_category).getOrElse("")

    (fCat ++ List(gCat) ++ List(wCat)).filter(_.nonEmpty)
  }

  private def extractCountryRegion(input: JoinedAll): (Option[(String, String)], Option[(String, String)], Option[(String, String)]) = {
    val wCountryRegion = input.w.flatMap(w => w.main_country.flatMap(country => w.main_region.map(region => (country, region))))
    val gCountryRegion = input.g.flatMap(g => g.country_name.flatMap(country => g.region_name.map(region => (country, region))))
    val fCountryRegion = input.f.flatMap(f => f.country_name.flatMap(country => f.region_name.map(region => (country, region))))
    (fCountryRegion, gCountryRegion, wCountryRegion)
  }
}
