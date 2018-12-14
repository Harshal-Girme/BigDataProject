package numberOfFlightsPerAirline.top30;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class BusiestSourceAirportsCustomWritable implements Writable, WritableComparable<BusiestSourceAirportsCustomWritable>{
	private String sourceId;
	private Float flightCount;
	
	public BusiestSourceAirportsCustomWritable() {
		
	}

	public BusiestSourceAirportsCustomWritable(String m, Float l) {
		this.sourceId = m;
		this.flightCount = l;
	}


	public String getSourceId() {
		return sourceId;
	}

	public void setSourceId(String hostId) {
		this.sourceId = hostId;
	}


	public Float getFlightCount() {
		return flightCount;
	}

	public void setFlightCount(Float listingCount) {
		this.flightCount = listingCount;
	}

	public void write(DataOutput d) throws IOException {
		d.writeUTF(sourceId);
		d.writeFloat(flightCount);
	}

	public void readFields(DataInput di) throws IOException {
		sourceId = di.readUTF();
		flightCount = di.readFloat();
	}
	
	public int compareTo(BusiestSourceAirportsCustomWritable o) {
		return -1 * (flightCount.compareTo(o.flightCount));
	}


	@Override
	public String toString() {
		return sourceId + "\t" + flightCount;
	}

}