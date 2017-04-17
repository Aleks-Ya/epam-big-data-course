package hive.hw3;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

@SuppressWarnings("unused")
public class UserAgentUdtf extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector objectInspectors) throws UDFArgumentException {
        return createObjectInspector();
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        if (objects == null) {
            return;
        }
        forward(processRow(objects));
    }

    String[] processRow(Object[] objects) {
        if (objects.length != 1) {
            throw new IllegalArgumentException("Expected 1 argument, but found " + objects.length);
        }
        String userAgent = PrimitiveObjectInspectorFactory.javaStringObjectInspector.getPrimitiveJavaObject(objects[0]);

        UserAgent agent = UserAgent.parseUserAgentString(userAgent);
        String browserData = agent.getBrowser().toString();
        String osData = agent.getOperatingSystem().toString();
        String deviceData = agent.getOperatingSystem().getDeviceType().toString();

        return new String[]{browserData, deviceData, osData};
    }

    @Override
    public void close() throws HiveException {
    }

    private static StandardStructObjectInspector createObjectInspector() {
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("browser");
        fieldNames.add("device");
        fieldNames.add("os");

        List<ObjectInspector> fieldOIs = Collections.nCopies(3, javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

}
