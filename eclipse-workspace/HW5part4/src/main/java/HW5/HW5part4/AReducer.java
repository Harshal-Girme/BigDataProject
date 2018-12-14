package HW5.HW5part4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AReducer extends Reducer<Text, CountAverageTuple, Text, CountAverageTuple> {
	private CountAverageTuple result = new CountAverageTuple();

	public void reduce(Text key, Iterable<CountAverageTuple> values,Context context)
			throws IOException, InterruptedException {

		float sum = 0;
		float count = 0;

		for (CountAverageTuple val : values) {
			sum += val.getCount() * val.getAverage();
			count += val.getCount();

		}
		result.setCount(count);
		result.setAverage(sum / count);
		context.write(key,result );
	}
}