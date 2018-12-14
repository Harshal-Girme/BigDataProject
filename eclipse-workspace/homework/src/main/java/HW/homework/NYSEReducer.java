package HW.homework;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NYSEReducer extends Reducer<Text, NYSEWritable, Text, NYSEWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<NYSEWritable> values,
			Reducer<Text, NYSEWritable, Text, NYSEWritable>.Context ctx) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		long maxVolume = 0;
        long minVolume = 0;
        boolean flag = true;
        float maxPriceAdj = 0.0f;
        NYSEWritable finalStockWritable = new NYSEWritable();
        for(NYSEWritable val : values){
            if(flag){
                minVolume = val.getStockVolume();
                maxVolume = val.getStockVolume();
                flag = false;
            }
            
            if(val.getMaxPriceAdjClose() > maxPriceAdj){
                maxPriceAdj = val.getMaxPriceAdjClose();
            }
            if(val.getStockVolume() > maxVolume){
                maxVolume = val.getStockVolume();
                finalStockWritable.setMaxStockVolumeDate(val.getMaxStockVolumeDate());
            }
            if(val.getStockVolume() < minVolume){
                minVolume = val.getStockVolume();
                finalStockWritable.setMinStockVolumeDate(val.getMinStockVolumeDate());
            }
        }
        finalStockWritable.setStockVolume(0);
        finalStockWritable.setMaxPriceAdjClose(maxPriceAdj);
        ctx.write(key, finalStockWritable);
	}
	
	
	
	

}
