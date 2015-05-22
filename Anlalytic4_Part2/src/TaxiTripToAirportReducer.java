import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*
 * Counting sum of journeys happened from a range of source to range of destination.
 */
public class TaxiTripToAirportReducer extends
		Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int noOfRoundTrips = 0;
		int missedRoundTrips = 0;
		boolean isRoundTrip = false;
		int buffer = 120;
		/*
		To group pickup and drop journeys from airport.
		*/
		List<String> pickUpAtAirport = new ArrayList<String>();
		List<String> dropAtAirport = new ArrayList<String>();
		//context.write(key, new Text("Please print in output"));
		while (values.iterator().hasNext()) {
			String singleValue = values.iterator().next().toString();
			if (singleValue.charAt(0) == 'P') {
				pickUpAtAirport.add(singleValue);
			}
			if (singleValue.charAt(0) == 'D') {
				dropAtAirport.add(singleValue);
			}
		}
		/*
		 * Check for each Cab medallion whether it includes pick up from airport or not.  
		 */
		for (String drop : dropAtAirport) {
			String dropAtArray[] = drop.split(",");
			String dropTime = dropAtArray[6].split(" ")[1];
			String[] hoursMin = dropTime.split(":");
			Integer timeInMinDrop = Integer.valueOf(hoursMin[0]) * 120
					+ Integer.valueOf(hoursMin[1]);
			for (String pick : pickUpAtAirport) {
				String pickAtArray[] = pick.split(",");
				String pickTime = pickAtArray[6].split(" ")[1];
				String[] hoursMinPick = pickTime.split(":");
				Integer timeInMinPick = Integer.valueOf(hoursMinPick[0]) * 120
						+ Integer.valueOf(hoursMinPick[1]);
				if (timeInMinDrop < timeInMinPick && timeInMinPick < timeInMinDrop + buffer) {
					/*
					 * To check for one medallion whether there is a round trip or not  
					 */
					noOfRoundTrips++;
					context.write(key, new Text(drop+ " " + noOfRoundTrips ));
					context.write(key, new Text(pick+" "+noOfRoundTrips));
					isRoundTrip = true;
					break;
				}
			}
			if (!isRoundTrip) {
				/* If its not a round trip then write skip record to output for count.
				 * This SkipRecrds count is used to count how many cabs missed the ride back from airport.
				 */
				missedRoundTrips++;
				context.write(key, new Text("Skiped Record "+missedRoundTrips));
				isRoundTrip = false;
				//We can write these values as well to output
			}
		}

	}
}
