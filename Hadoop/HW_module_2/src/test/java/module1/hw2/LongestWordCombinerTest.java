package module1.hw2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class LongestWordCombinerTest {
    @Test
    public void test() throws IOException {
        new MapDriver<Text, NullWritable, Text, NullWritable>()
                .withMapper(new LongestWordCombiner())
                .withAll(Arrays.asList(
                        new Pair<>(new Text("a"), NullWritable.get()),
                        new Pair<>(new Text("ccc"), NullWritable.get()),
                        new Pair<>(new Text("bb"), NullWritable.get())
                ))
                .withOutput(new Text("ccc"), NullWritable.get())
                .runTest();
    }
}
