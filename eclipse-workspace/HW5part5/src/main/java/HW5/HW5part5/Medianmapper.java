package HW5.HW5part5;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Medianmapper extends Mapper<LongWritable, Text, IntWritable,FloatWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		context.write(new IntWritable(Integer.valueOf(values[1])),new FloatWritable(Float.valueOf(values[2])));
}

}

//public class MovieMapper extends Mapper<LongWritable, Text, IntWritable,IntWritable>{
//    
//    @Override
//    public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException{
//       String[] tokens = value.toString().split(",");
//       ctx.write(new IntWritable(Integer.valueOf(tokens[1])),new IntWritable(Integer.valueOf(tokens[2])));
//    }
//}
