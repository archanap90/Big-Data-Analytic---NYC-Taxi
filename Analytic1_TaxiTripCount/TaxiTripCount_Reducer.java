/**
 * Reducer accumulates the count for each trip.
 */

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Counting sum of journeys happened from a range of source to range of destination.
 */
public class TaxiTripCount_Reducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		/*
			Count the number of rows sent by mapper to count number of trips between same source and destination.
			This Analytic helps you to find the most frequent rides between every source and destination pair.
		*/
		while (values.iterator().hasNext()) {
			sum += ((IntWritable) values.iterator().next()).get();
		}
		context.write(key, new IntWritable(sum));
	}
}
