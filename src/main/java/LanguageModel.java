import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		@Override
		public void setup(Context context) {
			// how to get the threashold parameter from the configuration?
			Configuration configuration = context.getConfiguration();
			threashold = configuration.getInt("threashold", 2);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			//how to filter the n-gram lower than threashold
			if (count < threashold)
				return;
			
			//this is --> cool = 20

			//what is the outputkey?
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < words.length - 1; i++)
				sb.append(words[i] + " ");
			String outputKey = sb.toString().trim();

			//what is the outputvalue?
			String outputVal = words[words.length - 1] + " = " + count;

			//write key-value to reducer?
            if (outputKey == null || outputKey.length() < 1)
                return;

			context.write(new Text(outputKey), new Text(outputVal));
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//can you use priorityQueue to rank topN n-gram, then write out to hdfs?
			TreeMap<Integer, List<String>> treemap = new TreeMap<Integer, List<String>>();
			for (Text val : values) {
			    String[] parts = val.toString().split("=");
			    String word = parts[0].trim();
			    int count = Integer.valueOf(parts[1].trim());

			    if (treemap.containsKey(count)) {
			        treemap.get(count).add(word);
                } else {
			        treemap.put(count, new ArrayList<String>());
			        treemap.get(count).add(word);
                }
            }

            Iterator<Integer> iter = treemap.keySet().iterator();
			for (int i = 0; i < n && i < treemap.size() && iter.hasNext(); i++) {
			    int count = iter.next();
			    List<String> words = treemap.get(count);
			    for (String curr : words) {
			        context.write(new DBOutputWritable(key.toString(), curr, count), NullWritable.get());
			        i++;
                }
            }

		}
	}
}
