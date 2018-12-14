package StockHigh;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockReducer extends Reducer<Text, DoubleWritable,Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		double min = Integer.MAX_VALUE, max = 0;

		Iterator<DoubleWritable> iterator = values.iterator(); //Iterating

		while (iterator.hasNext()) {

		double value = iterator.next().get();

		

		if (value > max) { //Finding max value

		max = value;

		} }

		
		context.write(new Text(key), new DoubleWritable(max));
	}

}
