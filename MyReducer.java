package Session04.Assignment01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MyReducer{
public static class TelevisionRecordsReducer extends Reducer<IntWritable, Text, Text, Text> {

	public void reduce(IntWritable keys, Text value,Context context) {}
	
}
	
}
