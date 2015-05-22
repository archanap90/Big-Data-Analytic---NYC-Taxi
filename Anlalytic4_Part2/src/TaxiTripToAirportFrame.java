import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
		This Analytic is to Find the frequency of cabs which gets pick up at airport.
*/
public class TaxiTripToAirportFrame {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Couldnt not read input from taxi data");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(TaxiTripToAirportFrame.class);
		job.setJobName("TaxiTripToAirport");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(TaxiTripToAirportMapper.class);
		job.setReducerClass(TaxiTripToAirportReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}