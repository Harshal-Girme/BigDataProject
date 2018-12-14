package HW5.part3;

import java.io.IOException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, LongWritable> {
	@Override
    public void map(LongWritable key, Text val, Context ctx) throws IOException, InterruptedException{
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy");
        String tokens[] = val.toString().split(" ");
        try {
        	CompositeKeyWritable alw = new CompositeKeyWritable(tokens[0],sdf.parse(tokens[3].substring(1)));
            ctx.write(alw,new LongWritable(1));
        } catch (Exception ex) {
           System.out.println(ex);
        }
        
    }

}
