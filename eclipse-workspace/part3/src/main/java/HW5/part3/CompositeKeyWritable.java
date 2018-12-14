package HW5.part3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {
	String ip;
    Date accessDate;

    public CompositeKeyWritable() {
    }

    public CompositeKeyWritable(String ip, Date accessDate) {
        this.ip = ip;
        this.accessDate = accessDate;
    }
    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Date getAccessDate() {
        return accessDate;
    }

    public void setAccessDate(Date accessDate) {
        this.accessDate = accessDate;
    }

    public void write(DataOutput d) throws IOException {
        d.writeUTF(ip);
        d.writeLong(accessDate.getTime());
    }

    public void readFields(DataInput di) throws IOException {
        this.ip = di.readUTF();
        this.accessDate = new Date(di.readLong());
    }
    
    public int compareTo(CompositeKeyWritable o) {
        int result = this.ip.compareTo(o.getIp());
        if(result == 0){
            result = -1* (this.accessDate.compareTo(o.getAccessDate()));
        }
        return result;
    }

    @Override
    public String toString() {
        return "AccessLogWritable{" + "ip=" + ip + ", accessDate=" + accessDate + '}';
    }

}


