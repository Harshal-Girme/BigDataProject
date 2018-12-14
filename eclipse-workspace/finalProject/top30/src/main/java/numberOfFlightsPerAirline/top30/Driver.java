package numberOfFlightsPerAirline.top30;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Usage: TopHostWithMostListings <input path> <intermediate> <output path>");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path output_intermediate = new Path(args[1]);
		Path output_dir = new Path(args[2]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = new Job(conf, "TopHostWithMostListings");
		job.setJarByClass(BusiestSourceAirportsMapper1.class);

		// Setup MapReduce
		job.setMapperClass(BusiestSourceAirportsMapper1.class);
		job.setReducerClass(BusiestSourceAirportsReducer1.class);

		// Specify key / value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, output_intermediate);

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(output_intermediate))
			hdfs.delete(output_intermediate, true);

		if (hdfs.exists(output_dir))
			hdfs.delete(output_dir, true);

		boolean complete = job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Chaining");

		if (complete) {
			// Setup MapReduce
			job2.setJarByClass(BusiestSourceAirportsMapper2.class);
			job2.setMapperClass(BusiestSourceAirportsMapper2.class);
			job2.setReducerClass(BusiestSourceAirportsReducer2.class);

			// Specify key / value
			job2.setMapOutputKeyClass(BusiestSourceAirportsCustomWritable.class);
			job2.setMapOutputValueClass(IntWritable.class);
			job2.setOutputKeyClass(BusiestSourceAirportsCustomWritable.class);
			job2.setOutputValueClass(NullWritable.class);
			// Input
			FileInputFormat.addInputPath(job2, output_intermediate);
			job2.setInputFormatClass(TextInputFormat.class);
			FileOutputFormat.setOutputPath(job2, output_dir);
			//System.exit(job2.waitForCompletion(true) ? 0 : 1);
			int code = job2.waitForCompletion(true) ? 0 : 1;
			System.exit(code);
		}
		
		
	}
}