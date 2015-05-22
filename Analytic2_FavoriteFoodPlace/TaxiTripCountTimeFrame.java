/**
 * Analytic 2: What is the Favorite food place?
 * Based on the drop off location during the lunch, breakfast and dinner time frame indicates
 * popular food place in the city.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class TaxiTripCountTimeFrame {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Couldnt not read input from taxi data");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(TaxiTripCountTimeFrame.class);
		job.setJobName("Trip Count");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(TaxiTripCountTimeFrame_Mapper.class);
		job.setReducerClass(TaxiTripCountTimeFrame_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}