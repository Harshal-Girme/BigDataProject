package HW5.HW5part5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianReducer extends Reducer<IntWritable, FloatWritable, IntWritable, MedianStdDevTuple> {
	
	@Override
	protected void reduce(IntWritable key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
		MedianStdDevTuple result = new MedianStdDevTuple();
		ArrayList<Float> ratings = new ArrayList<Float>();
		float sum = 0;
		int count = 0;
		float average = 0.0f;
		float median;
		float sumsquare = 0.0f;
		
		for (FloatWritable val : values) {
			ratings.add(val.get());
			sum += val.get();
			++count;
		}
		average = sum/count;
		
		if (ratings.size() % 2 == 0) {
			median = (ratings.get(count/2-1)+ratings.get(count/2))/2;

		} else {
			median = ratings.get(count / 2);
		}
		result.setMedian(median);

		for (Float f : ratings) {
			sumsquare  += (f - average) * (f - average);
		}

		result.setStdDev((float) Math.sqrt(sumsquare) / (count - 1));
		context.write(key, result);
	}
}
