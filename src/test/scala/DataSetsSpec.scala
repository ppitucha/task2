import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataSetsSpec extends AnyFlatSpec with Matchers {

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

  it should "calculate the score correctly" in {

    val input1 = JoinedAll.from(f1, None, None)
    val input2 = JoinedAll.from(f1, g1, None)
    val input3 = JoinedAll.from(f1, g1, w1)

    val score1 = JoinedAll.calculateScore(input1)
    val score2 = JoinedAll.calculateScore(input2)
    val score3 = JoinedAll.calculateScore(input3)

    score1 shouldEqual 100 // (100 for one defined source)
    score2 shouldEqual 230 // (200 two sources + 10 for two names defined + 20 for name similarities)
    score3 shouldEqual 365 // (300 three source + 15 for three names defined + 30 for name similarities + 20 for category similarities)
  }
}