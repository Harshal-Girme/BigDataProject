package HW5.HW5part4;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AMapper extends Mapper<Object, Text, Text, CountAverageTuple> {
	private CountAverageTuple outCountAverage = new CountAverageTuple();
	//private IntWritable stockYear = new IntWritable();
	//private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-mm-dd");

	@SuppressWarnings("deprecation")
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String values[] = value.toString().split(",");
		outCountAverage.setAverage(Float.valueOf(values[8]));
		outCountAverage.setCount(1);
		context.write(new Text(values[1]+ " "+values[3].substring(0, 4)), outCountAverage);
		}
	}
