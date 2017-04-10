package hive.hw3;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class UserAgentUdf extends UDF {
    public Text evaluate(final Text userAgentStr) {
        if (userAgentStr == null) {
            return null;
        }
        UserAgent userAgent = UserAgent.parseUserAgentString(userAgentStr.toString());
        return new Text(userAgent.toString());
    }
}
