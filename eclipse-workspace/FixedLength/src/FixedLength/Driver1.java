package FixedLength;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver1 {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Job job = Job.getInstance();
	    job.setJarByClass(FixedMapper.class);
	     
	    job.setInputFormatClass(NLineInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.setMapperClass(FixedMapper.class);
	    job.setReducerClass(FixedReducer.class);
	    
	    job.setNumReduceTasks(2);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    int code = job.waitForCompletion(true)? 0:1;
	    System.exit(code);
	    
	    Configuration conf = new Configuration(true);
	    
	    FileSystem hdfs = FileSystem.get(conf);
	    FixedLengthInputFormat.setRecordLength(conf, 5);
	    
	    Configuration conf1 = new Configuration();
	      conf1.setInt("mapreduce.input.lineinputformat.linespermap",4);
	    
        Path outputDir = new Path(args[1]);
		if (hdfs.exists(outputDir )) {
            hdfs.delete(outputDir, true);
        }
	}

}
