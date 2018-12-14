package wordCountDemo;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

public class Driver {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		// Create a new Job
	     Job job = Job.getInstance();
	     job.setJarByClass(WordCountMapper.class);
	}

}
