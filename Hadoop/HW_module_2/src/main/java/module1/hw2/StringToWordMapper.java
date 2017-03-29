package module1.hw2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class StringToWordMapper extends Mapper<NullWritable, Text, Text, NullWritable> {

    private Text word = new Text();

    @Override
    public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, NullWritable.get());
        }
    }
}
