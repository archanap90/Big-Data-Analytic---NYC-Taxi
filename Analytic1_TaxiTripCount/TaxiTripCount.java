/**
 * Analytic 1: Taxi trip count based on the starting and ending location of the trips. 
 * 				The count indicates the favorite pickup place and drop off place. 
 * 				Displaying the processed data on map gives appropriate information on the routes.
 */


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
*	This Analytic is to find the taxi trip count.
*
*
*
*/

public class TaxiTripCount {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Couldnt not read input from taxi data");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(TaxiTripCount.class);
		job.setJobName("Trip Count");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(TaxiTripCount_Mapper.class);
		job.setReducerClass(TaxiTripCount_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}