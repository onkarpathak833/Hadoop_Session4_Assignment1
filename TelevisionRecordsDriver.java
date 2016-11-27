package Session04.Assignment01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import Session04.Assignment01.*;
import Session04.Assignment01.MyMapper.TelevisionRecordsMapper;
//import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;
import Session04.Assignment01.MyReducer.TelevisionRecordsReducer;

public class TelevisionRecordsDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

			Configuration conf = new Configuration();
			Job job = new Job(conf);
			job.setJarByClass(TelevisionRecordsDriver.class);
			job.setMapperClass(TelevisionRecordsMapper.class);
			//job.setMapOutputKeyClass(Text.class);
			//job.setMapOutputValueClass(Text.class);
			job.setReducerClass(TelevisionRecordsReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.waitForCompletion(true);
	}

}
