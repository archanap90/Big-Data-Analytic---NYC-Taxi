import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 *  Analytic to find Each Taxi trip to airport is round trip or not.
 *  We have taken a buffer of "2 hours"(It could be easily modified) to check whether a cab has trip from airport to any other destination.
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
			String medallion = colValues[0];
			String pickUpDate = colValues[5].split(" ")[0];
			// check for airport location
			String pickupLatitude = colValues[11];// .substring(1);
			double pickupLat = Double.parseDouble(pickupLatitude);

			String pickupLongitude = colValues[10];
			double pickupLong = Double.parseDouble(pickupLongitude);

			String dropLatitude = colValues[13];// .substring(1);
			double dropLat = Double.parseDouble(dropLatitude);

			String dropLongitude = colValues[12];
			double dropLong = Double.parseDouble(dropLongitude);
			/*
			 *  Get only Airport locations whether it could be drop or pickup.s
			 */
			if (pickupLat < airportLatitudeMax
					&& pickupLat > airportLatitudeMin
					&& pickupLong > airportLongitudeMax
					&& pickupLong < airportLongitudeMin) {
				String mapKey = medallion + " " + pickUpDate;
				/*
				 * Key :::: is "Medallion + Date" to predict number of round
				 * trips for each cabs. Value :::: is the record along with
				 * whether its pick up from airport : "P" appended in the front
				 * of the value
				 */
				String newValue = "P," + singleLine;
				context.write(new Text(mapKey), new Text(newValue));
			}
			if (dropLat < airportLatitudeMax && dropLat > airportLatitudeMin
					&& dropLong > airportLongitudeMax
					&& dropLong < airportLongitudeMin) {
				String mapKey = medallion + pickUpDate;
				/*
				 * Key :::: is "Medallion + Date" to predict number of round
				 * trips for each cabs. Value :::: is the record along with
				 * whether its pick up from airport : "D" appended in the front
				 * of the value
				 */
				String newValue = "D," + singleLine;
				context.write(new Text(mapKey), new Text(newValue));
			}
		} catch (Exception e) {

		}
	}
}
