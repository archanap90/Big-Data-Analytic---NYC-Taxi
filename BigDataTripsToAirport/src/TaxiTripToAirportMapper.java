import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 *  First step is to count number of journeys happened between two source and destinations rounded values .
 *  We are considering source and destination latitude and longitude values rounded to group the near by areas .
 */

public class TaxiTripToAirportMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Double airportLatitudeMax = 40.651381;
		Double airportLatitudeMin = 40.640668;
		Double airportLongitudeMax = -73.776283;
		Double airportLongitudeMin = -73.794694;
		String singleLine = value.toString();
		String colValues[] = singleLine.split(",");

		try {
			String pickUpDate = colValues[5];
			// check for airport location
			String pickupLatitude = colValues[11];// .substring(1);
			double pickupLat = Double.parseDouble(pickupLatitude);

			String pickupLongitude = colValues[10];
			double pickupLong = Double.parseDouble(pickupLongitude);

			String dropLatitude = colValues[13];// .substring(1); double
			Double dropLat = Double.parseDouble(dropLatitude);

			String dropLongitude = colValues[12];
			double dropLong = Double.parseDouble(dropLongitude);

			if (dropLat < airportLatitudeMax && dropLat > airportLatitudeMin
					&& dropLong > airportLongitudeMax
					&& dropLong < airportLongitudeMin) {
				// String dropoffDate = colValues[8];
				String pickUpTime = pickUpDate.split("\\s+")[1];
				if (Integer.valueOf(pickUpTime.split(":")[1]) > 30) {
					pickUpTime = "30";
				} else {
					pickUpTime = "00";
				}
				String newValue = pickupLatitude + " " + pickupLongitude;
				String newKey = pickUpDate + " " + pickUpTime;
				context.write(new Text(newKey), new Text(newValue));
			}

		} catch (Exception e) {

		}
	}
}
