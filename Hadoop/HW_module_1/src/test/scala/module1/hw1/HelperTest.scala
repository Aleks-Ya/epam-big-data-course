package module1.hw1

import org.scalatest.{FlatSpec, Matchers}

class HelperTest extends FlatSpec with Matchers {

  it should "parse a data file's line" in {
    val line = """6ff912c0fa9b51dfeaa3a4d73bd4c1e5	20130612000102824	null	Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; Apache; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; Microsoft Windows Media Center PC 6.0; DianXin 1.3.6.1151),gzip(gfe),gzip(gfe)	60.190.0.*	94	96	2	trqRTJuSQqf7FmMs	de0b47024f107eb0811a662e99272bef		2886372919	336	280	2	0	5	dc0998c10f8f0b623b5d949e8272e4c7	238	3358	null"""
    val id = Helper.parseIPinYouID(line)
    id shouldEqual "20130612000102824"
  }

}

