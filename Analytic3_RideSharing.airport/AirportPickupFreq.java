/*
 *  Analytic 3: Airport Pickups are obtained by executing hiveQL on the data's external table. 
 *  This MapReduce is run on the results of HiveQL. 
 *  The destination location and time frame is considered
 *  to recommend RIDE SHARING from the airport.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirportPickupFreq {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Couldnt not read input from taxi data");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(AirportPickupFreq.class);
		job.setJobName("AirportPickupFreq");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(AirportPickupFreq_Mapper.class);
		job.setReducerClass(AirportPickupFreq_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}