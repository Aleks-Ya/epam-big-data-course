package hive.hw3;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@Ignore
public class UserAgentUdf2Test {

    @Test
    public void bot() throws HiveException {
        testUserAgent("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
                "BOT", "UNKNOWN", "");
    }

    @Test
    public void firefox() throws HiveException {
        testUserAgent("Mozilla/5.0 (Windows NT 5.1; rv:24.0) Gecko/20100101 Firefox/24.0",
                "FIREFOX24", "WINDOWS_XP", "24.0");
    }

    @Test
    public void chrome() throws HiveException {
        testUserAgent(" Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1 ",
                "CHROME21", "WINDOWS_XP", "21.0.1180.89");
    }

    private void testUserAgent(String userAgent, String expBrowser, String expOs, String expBrowserVersion) throws HiveException {
        UserAgentUdf2 agent2 = new UserAgentUdf2();
        StandardStructObjectInspector resultOI = agent2.initialize(
                new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaStringObjectInspector});

//        LazyObjectInspectorFactory.
        LazyStringObjectInspector argIO = LazyPrimitiveObjectInspectorFactory.getLazyStringObjectInspector(false, (byte) ' ');


        LazyString lazyUserAgent = new LazyString(argIO);
        byte[] userAgentBytes = userAgent.getBytes();
//        lazyUserAgent.init(userAgentBytes, 0, userAgentBytes.length - 1);
        GenericUDF.DeferredJavaObject object = new GenericUDF.DeferredJavaObject(lazyUserAgent);

        Object result = agent2.evaluate(new GenericUDF.DeferredObject[]{object});

        StructField browserField = resultOI.getStructFieldRef(UserAgentUdf2.browserFieldName);
        StructField browserVersionField = resultOI.getStructFieldRef(UserAgentUdf2.browserVersionFieldName);
        StructField osField = resultOI.getStructFieldRef(UserAgentUdf2.osFieldName);

        String browserFieldData = (String) resultOI.getStructFieldData(result, browserField);
        String browserVersionFieldData = (String) resultOI.getStructFieldData(result, browserVersionField);
        String osFieldData = (String) resultOI.getStructFieldData(result, osField);

        assertThat(browserFieldData, equalTo(expBrowser));
        assertThat(browserVersionFieldData, equalTo(expBrowserVersion));
        assertThat(osFieldData, equalTo(expOs));
    }
}