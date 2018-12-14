package HW5.HW5part5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;



public class MedianStdDevTuple implements Writable {

    float stdDev;
    float Median;
    
    

    public MedianStdDevTuple(float stdDev, float median) {
		super();
		this.stdDev = stdDev;
		this.Median = median;
	}
    
    

	public MedianStdDevTuple() {
		super();
	}



	public float getStdDev() {
        return stdDev;
    }

    public float getMedian() {
        return Median;
    }

    public void setStdDev(float stdDev) {
        this.stdDev = stdDev;
    }

    public void setMedian(float median) {
        this.Median = median;
    }

  
    @Override
    public String toString() {
        return "Std Dev = " + stdDev + "\t" + "Median = " + Median;
    }



	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeFloat(Median);
	    out.writeFloat(stdDev);
	}



	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		Median = in.readFloat();
        stdDev = in.readFloat();

	}


    


   

}