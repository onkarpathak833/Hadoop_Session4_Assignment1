package Session04.Assignment01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper{
public static class TelevisionRecordsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	public  void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("************ Inside Mapper");
		
		String line = value.toString();
		System.out.println(" *** "+line);
		if(line.length()==0) {
			System.exit(0);
		}
		int i= 0;
		String splitValues[] = line.split("\\|");
		//System.out.println("******** Split values are  : "+splitValues.length);
		
		String companyName = splitValues[0];
		String productName = splitValues[1];
		
		System.out.println("Company name and product name are : "+companyName+"   :  "+productName);
	
		
		if(companyName.equals("NA") || productName.equals("NA")) {
			
			System.out.println("Not writing invalid records.");
		}
		else {
			System.out.println("Writing valid records."+companyName+"  :  "+productName);
			context.write(new IntWritable(i),value);
			i++;
		}
	}
}
}
