package numberOfFlightsPerAirline.top30;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusiestSourceAirportsMapper2 extends Mapper<LongWritable, Text, BusiestSourceAirportsCustomWritable, IntWritable>{
	protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		Text sourceId = new Text();
		FloatWritable flightCount = new FloatWritable();
		if (values.toString().length() > 0) {
			try {
				String fields[] = values.toString().split("\t");
				sourceId.set(fields[0]);
				flightCount.set(Float.parseFloat(fields[1]));
				
				BusiestSourceAirportsCustomWritable data = new BusiestSourceAirportsCustomWritable(sourceId.toString(), flightCount.get());
				context.write(data, new IntWritable(1));
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
}