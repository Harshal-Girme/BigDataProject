package HW5.part3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
public class SPartitioner extends Partitioner <CompositeKeyWritable, LongWritable> {    
	@Override
    public int getPartition(CompositeKeyWritable key, LongWritable value, int numOfReducerTasks) {
        // TODO Auto-generated method stub
		int hash = key.getIp().hashCode();
        int partition = hash%numOfReducerTasks;
        return partition;
    }}

