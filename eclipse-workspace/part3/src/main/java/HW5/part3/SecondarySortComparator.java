package HW5.part3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortComparator extends WritableComparator {
	
	protected SecondarySortComparator(){
        super(CompositeKeyWritable.class, true);
    }
    
    @Override
    public int compare(WritableComparable alw1, WritableComparable alw2){
    	CompositeKeyWritable a = (CompositeKeyWritable) alw1;
    	CompositeKeyWritable b = (CompositeKeyWritable) alw2;
       int result = -1 * (a.getAccessDate().compareTo(b.getAccessDate()));
       if(result == 0){
            result = (a.getIp().compareTo(b.getIp()));
       }
        return result;
    }

}