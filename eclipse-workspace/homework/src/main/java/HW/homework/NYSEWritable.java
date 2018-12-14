package HW.homework;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class NYSEWritable implements Writable{
	
	private Date minStockVolumeDate;
    private Date maxStockVolumeDate;
    private long stockVolume;
    private float maxPriceAdjClose;
    
    public NYSEWritable(Date minStockVolumeDate, Date maxStockVolumeDate, long StockVolume, float maxPriceAdjClose) {
        super();
        this.minStockVolumeDate = minStockVolumeDate;
        this.maxStockVolumeDate = maxStockVolumeDate;
        this.stockVolume = StockVolume;
        this.maxPriceAdjClose = maxPriceAdjClose;
    }

    public NYSEWritable() {
        super();
    }

    public Date getMinStockVolumeDate() {
        return minStockVolumeDate;
    }

    public void setMinStockVolumeDate(Date minStockVolumeDate) {
        this.minStockVolumeDate = minStockVolumeDate;
    }

    public Date getMaxStockVolumeDate() {
        return maxStockVolumeDate;
    }

    public void setMaxStockVolumeDate(Date maxStockVolumeDate) {
        this.maxStockVolumeDate = maxStockVolumeDate;
    }

    public long getStockVolume() {
        return stockVolume;
    }

    public void setStockVolume(long stockVolume) {
        this.stockVolume = stockVolume;
    }

    public float getMaxPriceAdjClose() {
        return maxPriceAdjClose;
    }

    public void setMaxPriceAdjClose(float maxPriceAdjClose) {
        this.maxPriceAdjClose = maxPriceAdjClose;
    }

    

    public void write(DataOutput d) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
        d.writeLong(this.minStockVolumeDate.getTime());
        d.writeLong(this.maxStockVolumeDate.getTime());
        d.writeLong(this.stockVolume);
        d.writeFloat(this.maxPriceAdjClose);
    }

    
    public void readFields(DataInput di) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
        this.minStockVolumeDate = new Date(di.readLong());
        this.maxStockVolumeDate = new Date(di.readLong());
        this.stockVolume = di.readLong();
        this.maxPriceAdjClose = di.readFloat();
    }
    
    @Override
    public String toString() {
        return "minStockVolumeDate=" + minStockVolumeDate + ", maxStockVolumeDate=" + maxStockVolumeDate +  ", maxPriceAdjClose=" + maxPriceAdjClose ;
    }
	
	

}
