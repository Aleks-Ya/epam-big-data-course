package lesson1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.FlatSpec
import org.scalatest.Matchers.convertToStringShouldWrapper
import org.scalatest.BeforeAndAfterAll

class SmallDataTest extends FlatSpec with BeforeAndAfterAll {

  var sc: SparkContext = null

  override def beforeAll() {
    val conf = new SparkConf().setAppName("SmallDataTest").setMaster("local")
    sc = new SparkContext(conf)
  }

  "Process lines from the access log" should "return Top5" in {
    val source = Array(
      """ip42 - - [24/Apr/2011:06:32:37 -0400] "GET / HTTP/1.0" 200 6530 "-" "WebMoney/1.0 (AdvisorBot 0.1)/Nutch-1.2"""",
      """ip1 - - [24/Apr/2011:06:32:48 -0400] "GET /~strabal/grease/photo8/T926-8.jpg HTTP/1.1" 200 5980 "-" "Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)"""",
      """ip43 - - [24/Apr/2011:06:34:33 -0400] "GET / HTTP/1.1" 200 9550 "-" "Java/1.6.0_04"""",
      """ip43 - - [24/Apr/2011:06:34:34 -0400] "GET /about.html HTTP/1.1" 200 4981 "-" "Java/1.6.0_04"""",
      """ip27 - - [24/Apr/2011:05:36:32 -0400] "GET /images/title4.gif HTTP/1.1" 200 4678 "http://host3/" "Opera/9.80 (Windows NT 5.1; U; en) Presto/2.6.30 Version/10.62"""",
      """ip43 - - [24/Apr/2011:06:35:18 -0400] "GET /sgi_indigo/ HTTP/1.1" 200 9048 "-" "Java/1.6.0_04"""",
      """ip56 - - [24/Apr/2011:07:05:00 -0400] "GET /robots.txt HTTP/1.0" 404 274 "-" "Mozilla/5.0 ()"""",
      """ip25 - - [24/Apr/2011:08:34:42 -0400] "GET / HTTP/1.1" 200 150 "-" "Baiduspider+(+http://www.baidu.com/search/spider.htm)"""")

    val lines = sc.parallelize(source)

    val al = new AccessLogTask()
    al.processLines(sc, lines)
    val top5 = al.getTop5()
    println("Top5:\n" + top5)

    val expected =
      """ip43,7859,23579
ip42,6530,6530
ip1,5980,5980
ip27,4678,4678
ip56,274,274
"""
    top5 shouldEqual expected

  }

  override def afterAll() {
    sc.stop()
  }
}