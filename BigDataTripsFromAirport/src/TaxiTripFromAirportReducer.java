import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Counting sum of journeys happened from a range of source to range of destination.
 */
public class TaxiTripFromAirportReducer extends
		Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		while (values.iterator().hasNext()) {
			context.write(key, new Text(values.iterator().next()));
		}
		
	}
}
