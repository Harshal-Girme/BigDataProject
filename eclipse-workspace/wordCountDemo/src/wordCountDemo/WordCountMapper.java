package wordCountDemo;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

}
