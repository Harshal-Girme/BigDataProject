package HW.homework;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.protobuf.TextFormat.ParseException;

public class NYSEMapper extends Mapper<LongWritable, Text, Text,NYSEWritable>{

	@Override
    public void map(LongWritable key, Text val, Context ctx) throws IOException, InterruptedException{
        String[] tokens = val.toString().split(",");
        SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd");
        try {
            NYSEWritable stw = new NYSEWritable(sdf.parse(tokens[2]), sdf.parse(tokens[2]), Long.valueOf(tokens[7]), Float.valueOf(tokens[8]));
            System.out.println(stw);
            ctx.write(new Text(tokens[1]), stw);
        } catch (ParseException ex) {
            System.out.println(ex);
        } catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }


	
}
