/*
 *  Mapper parses data and determine the trip count in each time frame for a particular
 *  drop off location.
 */

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class FlightCount_Mapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	public static boolean skipFirstRow =true;
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String singleLine = value.toString();
		String colValues[] = singleLine.split("\\s+|,");
		
		try{
		String[] FlightDay = colValues[0].split("/|-");
		String[] FlightTime = colValues[5].split(":");

		String FlightTiming =   FlightDay[1] +","+ String.valueOf((Integer.valueOf(FlightTime[0])))+"-"+ String.valueOf((Integer.valueOf(FlightTime[0])+1));
		context.write(new Text(FlightTiming), new IntWritable(1));
		}catch(Exception e){
			
		}
	}
		
}
