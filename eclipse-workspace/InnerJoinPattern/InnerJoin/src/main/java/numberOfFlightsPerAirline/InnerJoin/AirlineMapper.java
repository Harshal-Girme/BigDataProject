package numberOfFlightsPerAirline.InnerJoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AirlineMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		String airlineId = values[0];
		
		outkey.set(airlineId);
		outvalue.set("A|" + value);
		context.write(outkey, outvalue);
	}
}
