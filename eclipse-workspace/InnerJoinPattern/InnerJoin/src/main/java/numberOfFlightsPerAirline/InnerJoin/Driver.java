package numberOfFlightsPerAirline.InnerJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
	public static void main(String args[]) throws Exception {

		if (args.length != 3) {
			System.err.println(
					"Usage: <input_Routes> <input_Airlines> <output>");
			System.exit(2);
		}
		Path routesInput = new Path(args[0]);
		Path airlinesInput = new Path(args[1]);
		Path outputDir = new Path(args[2]);

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Join ReduceSide");
		job.setJarByClass(Driver.class);

		MultipleInputs.addInputPath(job, routesInput, TextInputFormat.class, RouteMapper.class);
		MultipleInputs.addInputPath(job, airlinesInput, TextInputFormat.class, AirlineMapper.class);
		job.getConfiguration().set("join.type", "inner");
		job.setReducerClass(AirlineRouteReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		System.exit(job.waitForCompletion(true) ? 0 : 2);

	}
}
