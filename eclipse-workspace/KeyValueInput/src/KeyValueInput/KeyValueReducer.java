package KeyValueInput;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeyValueReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
	private IntWritable total = new IntWritable();
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Reducer<IntWritable,IntWritable,IntWritable,IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0;
		for(IntWritable i : values) {
			sum += i.get();
		}
		
		total.set(sum);
		context.write(key, total);
	}

}
