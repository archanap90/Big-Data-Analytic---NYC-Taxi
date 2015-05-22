/**
 * Mapper process Airport pickups, to group journeys based on the nearby drop off points
 * and the time. 
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 *  First step is to count number of journeys happened between two source and destinations rounded values .
 *  We are considering source and destination latitude and longitude values rounded to group the near by areas .
 */

public class AirportPickupFreq_Mapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	public static boolean skipFirstRow =true;
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if(skipFirstRow == false){
			
		try{
		String singleLine = value.toString();
		String colValues[] = singleLine.split("\\s+|,");
		String[] pickupDate = colValues[6].split(":");
		String dropLatitude = colValues[14];
		double dropLat = Math.round(Double.parseDouble(dropLatitude)*1000)/1000.0;
		String dropLongitude = colValues[15];
		double dropLong = Math.round(Double.parseDouble(dropLongitude)*1000)/1000.0;
		/*
			Key is the date along with pickup date anddrop coordinates
		*/
		String mapKey = pickupDate[0] +"-"+ String.valueOf((Integer.valueOf(pickupDate[0])+1))+ ","+","+ String.valueOf(dropLat) +","+ String.valueOf(dropLong)+",";

		context.write(new Text(mapKey), new IntWritable(1));
		}catch(Exception e){
			
		}
	}
		else
				skipFirstRow =false;
	}
	
}
