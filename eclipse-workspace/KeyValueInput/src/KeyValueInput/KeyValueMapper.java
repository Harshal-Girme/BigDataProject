package KeyValueInput;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KeyValueMapper extends Mapper<IntWritable,IntWritable,IntWritable,IntWritable>{

	@Override
	protected void map(IntWritable key, IntWritable value, Mapper<IntWritable,IntWritable,IntWritable,IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

}
