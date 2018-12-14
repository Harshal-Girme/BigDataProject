package HW5.part3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SReducer extends Reducer<CompositeKeyWritable, LongWritable, CompositeKeyWritable, LongWritable> {
	@Override
	protected void reduce(CompositeKeyWritable key, Iterable<LongWritable> values,
			Reducer<CompositeKeyWritable, LongWritable, CompositeKeyWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		for (LongWritable val : values) {
			try {				
				context.write(key, val);
			} catch (Exception e) {
				System.out.println("Here is the exception" + e);
			}
		}
	}
}
