package HW5.part3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
    
public class GroupingComparator extends WritableComparator{
	    
	    public GroupingComparator(){
	        super(CompositeKeyWritable.class, true);
	    }
	    @Override
	    public int compare(WritableComparable a, WritableComparable b){
	    	CompositeKeyWritable alw1 = (CompositeKeyWritable)a;
	    	CompositeKeyWritable alw2 = (CompositeKeyWritable)b;
	        
	        return alw1.getIp().compareTo(alw2.getIp());
	    }
}
