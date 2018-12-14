package FixedLength;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FixedMapper extends Mapper<IntWritable,IntWritable,IntWritable,IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	@Override
	protected void map(IntWritable key, IntWritable value,
			Mapper<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] fields = value.toString().split(" ");
		for(String s : fields) {
			Text t = new Text(s);
			int i = Integer.parseInt(t.toString());
			context.write(new IntWritable(i), one);
			break;
		}
	}

}