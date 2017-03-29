package module1.hw2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LongestWordCombiner extends Mapper<Text, NullWritable, Text, NullWritable> {

    private String longestWord = "";

    @Override
    public void map(Text word, NullWritable nothing, Context context) throws IOException {
        if (word.getLength() > longestWord.length()) {
            longestWord = word.toString();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text(longestWord), NullWritable.get());
    }
}
