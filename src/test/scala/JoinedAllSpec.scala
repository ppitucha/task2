import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JoinedAllSpec extends AnyFlatSpec with Matchers {

  val f1: Option[Facebook] = Some(Facebook(Some("deepelectricalsupply.com"), Some("3820 97 street nw, t6e5s8, edmonton, ab, canada, alberta"),
    Some("Malls & Shopping Centers"), Some("edmonton"), Some("ca"), Some("canada"), None, None,
    Some("https://deepelectricalsupply.com"), Some("Deep Electrical Supply"), Some("Organization"),
    Some("+17802453880"), Some("ca"), Some("ab"), Some("alberta"), None))

  val g1: Option[Google] = Some(Google(Some("3820 97 St NW, Edmonton, AB T6E 5S8, Canada"), Some("Electric Supplies & Power Generation"),
    Some("edmonton"), Some("ca"), Some("canada"), Some("Deep Electrical Supply Ltd."), Some("+17802453880"), Some("ca"),
    Some("3820 97 St NW · In Strathcona Business Park"), Some("+1 780-245-3880"), Some("ab"), Some("alberta"),
    Some("5.0 (28) · Electrical supply store 3820 97 St NW · In Strathcona Business Park Closed ⋅ Opens 8AM Mon · +1 780-245-3880 \" Highly recommend coming to them  for all your electrical needs !\" In-store shopping·Curbside pickup·Delivery"),
    Some("t6e 5s8"), Some("deepelectricalsupply.com")))

  val w1: Option[Website] = Some(Website(Some("deepelectricalsupply.com"), Some("com"), Some("en"), Some("Deep Electrical Supply Ltd"),
    Some("edmonton"), Some("canada"), Some("alberta"), Some("17802453880"), Some("Deep Electrical Supply"),
    Some("com"), Some("Electric Supplies & Power Generation")))


  val f2: Option[Facebook] = Some(Facebook(Some("alc-calgary.ca"), Some("3325 49 st sw, t3e6m6, calgary, ab, canada, alberta"),
    Some("Churches & Religious Organizations|Churches & Religious Organizations|Churches & Religious Organizations"),
    Some("calgary"), Some("ca"), Some("canada"), None, None, Some("https://alc-calgary.ca"), Some("Abundant Life Church"),
    Some("Organization"), Some("+14032461804"), Some("ca"), Some("ab"), Some("alberta"), None))

  val g2: Option[Google] = Some(Google(Some("3343 49 St SW, Calgary, AB T3E 6M6, Canada"), Some("Churches & Religious Organizations"), Some("calgary"), Some("ca"), Some("canada"), Some("Abundant Life Church"), Some("+14032461804"), Some("ca"), Some("3343 49 St SW"), Some("+1 403-246-1804"), Some("ab"), Some("alberta"), Some("5.0 (1)  ·Christian church 3343  49 St SW Temporarily closed  ·+1 403 - 246 - 1804 "),
    Some(" t3e 6m6"),Some(" alc -calgary. ca")))

  val w2: Option[Website] = Some(Website(Some("alc-calgary.ca"), Some("ca"), Some("en"), None, Some("calgary"),
    Some("canada"), Some("alberta"), Some("14032461804"), Some("abundant life church"), Some("ca"),
    Some("Churches & Religious Organizations")))


  it should "calculate the score correctly" in {
    val input11 = JoinedAll.from(f1, None, None)
    val input12 = JoinedAll.from(f1, g1, None)
    val input13 = JoinedAll.from(f1, g1, w1)

    val input21 = JoinedAll.from(None, None, w2)
    val input22 = JoinedAll.from(None, g2, w2)
    val input23 = JoinedAll.from(f2, g2, w2)

    input11.toFinalData.score shouldEqual 14
    input12.toFinalData.score shouldEqual 40
    input13.toFinalData.score shouldEqual 64

    input21.toFinalData.score shouldEqual 14
    input22.toFinalData.score shouldEqual 44
    input23.toFinalData.score shouldEqual 66
  }
}