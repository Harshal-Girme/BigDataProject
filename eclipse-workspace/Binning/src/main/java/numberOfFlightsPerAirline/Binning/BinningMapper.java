package numberOfFlightsPerAirline.Binning;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class BinningMapper extends Mapper<Object, Text, Text, NullWritable> {
	private MultipleOutputs<Text, NullWritable> mos = null;

	protected void setup(Context context) {
		mos = new MultipleOutputs(context);
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] row = value.toString().split(",");
		String country = row[6];
		country = country.replace("\"", "");
		System.out.println(country);
		
		if (country.equalsIgnoreCase("United States")) {
			mos.write("bins", value, NullWritable.get(), "United States");
		}
		if (country.equalsIgnoreCase("Canada")) {
			mos.write("bins", value, NullWritable.get(), "Canada");
		}
		if (country.equalsIgnoreCase("United Kingdom")) {
			mos.write("bins", value, NullWritable.get(), "United Kingdom");
		}
		if (country.equalsIgnoreCase("United Arab Emirates")) {
			mos.write("bins", value, NullWritable.get(), "United Arab Emirates");
		}
		if (country.equalsIgnoreCase("Russia")) {
			mos.write("bins", value, NullWritable.get(), "Russia");
		}
		if (country.equalsIgnoreCase("Australia")) {
			mos.write("bins", value, NullWritable.get(), "Australia");
		}
		if (country.equalsIgnoreCase("Mexico")) {
			mos.write("bins", value, NullWritable.get(), "Mexico");
		}
		if (country.equalsIgnoreCase("India")) {
			mos.write("bins", value, NullWritable.get(), "India");
		}
		

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}