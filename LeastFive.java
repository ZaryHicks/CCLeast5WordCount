import java.io.*; 
import java.util.*; 

import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;

public class LeastFive {

	public static class Least_Five_Mapper extends Mapper<Object, Text, Text, IntWritable> { 
			
		private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	StringTokenizer itr = new StringTokenizer(value.toString());
	    	while (itr.hasMoreTokens()) {
	    		word.set(itr.nextToken());
	    		context.write(word, one);
	    	}
	    }
	}
	
	public static class Least_Five_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
		
		// Custom comparator to have the first key be the largest
		private class myComp implements Comparator<Integer>{
			public int compare(Integer a, Integer b) {
				//opposite of natural ordering
				if(a < b) {
					return 1;
				}
				else if(a > b){
					return -1;
				}
				else {
					return 0;
				}
			}
		}
		
		// TreeMap for storing/sorting least 5
		private TreeMap<Integer, String> map2; 

		@Override
		public void setup(Context context) throws IOException, InterruptedException{ 
			map2 = new TreeMap<Integer, String>(new myComp());
		} 

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
		
			int sum = 0;
		    for (IntWritable val : values) {
		        sum += val.get();
		    }
		    
		    // put in map, custom compare should put largest Value first
		 	map2.put(sum, key.toString()); 
			
			// we remove the first key-value (first should be largest key (largest count))
			// if we have more than 5
			if (map2.size() > 5){ 
				map2.remove(map2.firstKey()); 
			} 
		} 

		private Text word = new Text();
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException { 
			for (Map.Entry<Integer, String> entry : map2.entrySet()) { 		
				IntWritable count = new IntWritable(entry.getKey()); 
				word.set(entry.getValue()); 
				context.write(word, count); 
			} 
		} 
	}
	
	public static void main(String[] args) throws Exception { 
        Configuration conf = new Configuration(); 
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
  
        // if less than two paths  
        // provided will show error 
        if (otherArgs.length < 2){ 
            System.err.println("Error: please provide two paths"); 
            System.exit(2); 
        } 
  
        Job job = Job.getInstance(conf, "Least 5"); 
        job.setJarByClass(LeastFive.class);
        job.setNumReduceTasks(1);
  
        job.setMapperClass(Least_Five_Mapper.class);
        //job.setCombinerClass(Least_Five_Reducer.class);
        job.setReducerClass(Least_Five_Reducer.class); 
  
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
  
        FileInputFormat.addInputPath(job, new Path(otherArgs[1])); 
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2])); 
  
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
}
