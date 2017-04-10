package hive.hw3;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class UserAgentUdf2Test {

    @Test
    public void evaluate() throws Exception {
        UserAgentUdf2 agent2 = new UserAgentUdf2();
        JavaStringObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
//        StandardListObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        StandardStructObjectInspector resultOI = agent2.initialize(new ObjectInspector[]{stringOI});

        String userAgent = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
        GenericUDF.DeferredJavaObject object = new GenericUDF.DeferredJavaObject(userAgent);
//        new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(list), object};
        Object result = agent2.evaluate(new GenericUDF.DeferredObject[]{object});
        StructField browserField = resultOI.getStructFieldRef(UserAgentUdf2.browserFieldName);
        StructField browserVersionField = resultOI.getStructFieldRef(UserAgentUdf2.browserVersionFieldName);
        StructField osField = resultOI.getStructFieldRef(UserAgentUdf2.osFieldName);
        String browserFieldData = (String) resultOI.getStructFieldData(result, browserField);
        String browserVersionFieldData = (String) resultOI.getStructFieldData(result, browserVersionField);
        String osFieldData = (String) resultOI.getStructFieldData(result, osField);
        assertThat(browserFieldData, equalTo("BOT"));
        assertThat(browserVersionFieldData, equalTo(""));
        assertThat(osFieldData, equalTo("UNKNOWN"));
    }
}