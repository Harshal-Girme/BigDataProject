package HW5.HW5part4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CountAverageTuple implements Writable {
	private float count = 0;
	private float Average = 0;
	
	

	public CountAverageTuple() {
		super();
		// TODO Auto-generated constructor stub
	}

	public float getCount() {
		return count;
	}

	public float getAverage() {
		return Average;
	}

	public void setCount(float count) {
		this.count = count;
	}

	public void setAverage(float average) {
		Average = average;
	}

	public void readFields(DataInput in) throws IOException {
		count = in.readFloat();
		Average = in.readFloat();

	}

	public void write(DataOutput out) throws IOException {
		out.writeFloat(getCount());
		out.writeFloat(getAverage());

	}

	@Override
    public String toString() {
        return "Count = " + count + " Average = " + Average;
    }

}