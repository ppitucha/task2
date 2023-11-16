import JoinedAll.{mergeCategory, mergeCountyAndRegion, mergeName, mergePhone}

import scala.util.matching.Regex

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

trait SourceDataset {
  private val domainRegexp: Regex = ("^(((?!-))(xn--|_)?[a-z0-9-]{0,61}[a-z0-9]{1,1}\\.)*(xn--)?([a-z0-9][a-z0-9\\-]{0,60}|[a-z0-9-]{1,30}\\.[a-z]{2,})$").r

  protected def validateDomain(domain: Option[String]): Boolean =
    domain.isDefined && domainRegexp.matches(domain.get)

  def hasValidDomain: Boolean
}

