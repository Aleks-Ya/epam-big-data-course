package hive.hw3;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;

import static org.hamcrest.Matchers.arrayContaining;
import static org.junit.Assert.assertThat;

public class UserAgentUdtfTest {
    @Test
    public void process() throws HiveException {
        String agentStr = "Mozilla/5.0 (Windows NT 5.1; rv:24.0) Gecko/20100101 Firefox/24.0";
        UserAgentUdtf udtf = new UserAgentUdtf();
        Object[] result = udtf.processRow(new Object[]{agentStr});
        assertThat(result, arrayContaining("FIREFOX24", "COMPUTER", "WINDOWS_XP"));
    }

}