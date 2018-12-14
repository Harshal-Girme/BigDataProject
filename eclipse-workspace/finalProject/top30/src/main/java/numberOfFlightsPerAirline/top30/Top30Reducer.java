package numberOfFlightsPerAirline.top30;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top30Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

	private TreeMap<String, Text> repToRecordMap = new TreeMap<String, Text>();

	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			String[] row = value.toString().split(",");
			//int availability365 = Integer.parseInt(row[4]);
			String active = row[7];
			repToRecordMap.put(active, new Text(value));

			if (repToRecordMap.size() > 30) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		for (Text t : repToRecordMap.descendingMap().values()) {
			context.write(NullWritable.get(), t);
		}
	}
}