package StockHigh;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockHighMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String [] fields = value.toString().split(",");
		Text t = new Text(fields[1]);
		context.write(t, new DoubleWritable(Double.parseDouble(fields[4])));
	}

}
