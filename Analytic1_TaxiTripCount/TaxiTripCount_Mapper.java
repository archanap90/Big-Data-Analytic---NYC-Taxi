import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 *  First step is to count number of journeys happened between two source and destinations rounded values .
 *  We are considering source and destination latitude and longitude values rounded to group the near by areas .
 */

public class TaxiTripCount_Mapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	public static boolean skipFirstRow =true;
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if(skipFirstRow == false){
			
		try{
		String singleLine = value.toString();
		String colValues[] = singleLine.split("\\s+|,");
		/*
			We need to round off location values in order to find near by locations . 
			Two near by locations have most of the digits in common but the last digits could vary as they are not exactly same location.
		*/
		String pickupLatitude = colValues[12];
		double pickupLat = Math.round(Double.parseDouble(pickupLatitude)*100000)/100000.0;
		String pickupLongitude = colValues[13];
		double pickupLong = Math.round(Double.parseDouble(pickupLongitude)*100000)/100000.0;
		String dropLatitude = colValues[14];
		double dropLat = Math.round(Double.parseDouble(dropLatitude)*100000)/100000.0;
		String dropLongitude = colValues[15];
		double dropLong = Math.round(Double.parseDouble(dropLongitude)*100000)/100000.0;
		/*
			Key is source + destination combination because to find all the trips between those locations.
		*/
		String mapKey = String.valueOf(pickupLat) + ","+String.valueOf(pickupLong)
				+","+ String.valueOf(dropLat) +","+ String.valueOf(dropLong)+",";

		context.write(new Text(mapKey), new IntWritable(1));
		}catch(Exception e){
			
		}
	}
		else
				skipFirstRow =false;
	}
	
}
