/**
 * Reducer groups the trips made to common location in the time-frame. 
 * This information is used to recommend Ride Sharing possiblity.
 */
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Counting sum of journeys happened from a range of source to range of destination.
 */
public class AirportPickupFreq_Reducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		/*
			Sum of journeys from mapper
		*/
		while (values.iterator().hasNext()) {
			sum += ((IntWritable) values.iterator().next()).get();
		}
		context.write(key, new IntWritable(sum));
	}
}
