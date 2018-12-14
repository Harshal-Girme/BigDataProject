package HW5.HW5part6;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MMedianmapper extends Mapper<LongWritable, Text, IntWritable, SortedMapWritable> {
	@Override
    public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException{
       String[] tokens = value.toString().split(",");
       SortedMapWritable ratingMap = new SortedMapWritable();
       ratingMap.put(new FloatWritable(Float.valueOf(tokens[2])), new IntWritable(1));
       ctx.write(new IntWritable(Integer.valueOf(tokens[1])),ratingMap);
    }
}