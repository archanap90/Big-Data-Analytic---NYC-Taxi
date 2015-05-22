/*
 *  Mapper parses data and determine the trip count in each time frame for a particular
 *  drop off location.
 */

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaxiTripCountTimeFrame_Mapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	public static boolean skipFirstRow = true;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (skipFirstRow == false) {
			String singleLine = value.toString();
			String colValues[] = singleLine.split("\\s+|,");

			try {
				String[] pickupDate = colValues[6].split(":");
				String dropLatitude = colValues[14];
				double dropLat = Double.parseDouble(dropLatitude);
				String dropLongitude = colValues[15];
				double dropLong = Double.parseDouble(dropLongitude);
				// String dropoffDate = colValues[8];
				/*
					Check whether the location is in NY region.As some of the records are corrupted .
				*/
				if (dropLat < Double.valueOf("40.917577")
						&& dropLong > Double.valueOf("-74.25909")
						&& dropLat > Double.valueOf("40.477399")
						&& dropLong < Double.valueOf("-73.700272")) {
					String pickUpBuffer = pickupDate[0]
							+ "-"
							+ String.valueOf((Integer.valueOf(pickupDate[0]) + 1))
							+ "\t" + dropLat + "\t" + dropLong;
							/*
								Key is the date and drop coordinates
							*/
					context.write(new Text(pickUpBuffer), new IntWritable(1));
				}
			} catch (Exception e) {

			}
		} else
			skipFirstRow = false;
	}
}
