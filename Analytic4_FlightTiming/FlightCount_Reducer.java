/*
 *  Reducer analyzes result obtained by Mapper and generates the total count in the time frame
 *  for a particular destination.
 *  Reducer output is further processed using PigScript to get the favorite food place.
 */

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Counting sum of journeys happened from a range of source to range of destination.
 */
public class FlightCount_Reducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		while (values.iterator().hasNext()) {
			sum += ((IntWritable) values.iterator().next()).get();
		}
		context.write(key, new IntWritable(sum));
	}
}
