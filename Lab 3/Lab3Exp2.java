import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Lab3Exp2 {
	private static int[][] boundaries = new int[10][2];
	private static int totalSamples = 0;
	public static void main(String[] args) throws Exception{ 
	
		///////////////////////////////////////////////////
		///////////// First Round MapReduce ///////////////
		////// where you might want to do some sampling ///
		///////////////////////////////////////////////////
		int reduceNumber = 1;
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: Patent <in> <out>");
			System.exit(2);
		}
		
		//Check which input file is being used and set the sample number accordingly
		String[] inputDir = otherArgs[0].split("/");
		String inputFile = inputDir[inputDir.length - 1];
		String fileSize = inputFile.split("-")[1];
		if(fileSize.equals("5m")) {
			totalSamples = 100000;
		}
		else if(fileSize.equals("500k")) {
			totalSamples = 10000;
		}
		else if(fileSize.equals("50k")) {
			totalSamples = 1000;
		}
		else if(fileSize.equals("5k")) {
			totalSamples = 100;
		}
		else {
			totalSamples = 10000;
		}
		
		
		Job job = Job.getInstance(conf, "Exp2");
		
		job.setJarByClass(Lab3Exp2.class);
		job.setNumReduceTasks(reduceNumber);
	
		job.setMapperClass(mapOne.class);
		job.setReducerClass(reduceOne.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		FileOutputFormat.setOutputPath(job, new Path("/labs/lab3/exp2/temp"));
		
		job.waitForCompletion(true);
		
		//Read each line from the first MapReduce job
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path("/labs/lab3/exp2/temp/part-r-00000"))));
		String line = reader.readLine();
		
		int count = 0;
		int valueOne = (int)line.split("\\s+")[1].charAt(0);
		int valueTwo = valueOne;
		int boundaryIndex = 0;
		
		//Generate the boundaries
		while(line != null) {
			String[] split = line.split("\\s+");
			count += Integer.parseInt(split[0]);
			//Check if the rolling sum is 10 percent of the total samples
			if(count >= totalSamples / 10 && boundaryIndex < 10) {
				//If it is generate the boundary and reset the sum and first value
				count -= totalSamples / 10;
				valueTwo = (int)split[1].charAt(0);
				boundaries[boundaryIndex] = new int[]{valueOne, valueTwo};
				boundaryIndex++;
				valueOne = valueTwo + 1;
			}
			line = reader.readLine();
		}
		
		///////////////////////////////////////////////////
		///////////// Second Round MapReduce //////////////
		///////////////////////////////////////////////////
		Job job_two = Job.getInstance(conf, "Round Two");
        job_two.setJarByClass(Lab3Exp2.class);
        
        conf.setInt("Count", 0);
        // Providing the number of reducers for the second round
        reduceNumber = 10;
        job_two.setNumReduceTasks(reduceNumber);

        // Should be match with the output datatype of mapper and reducer
        job_two.setMapOutputKeyClass(Text.class);
        job_two.setMapOutputValueClass(Text.class);
         
        job_two.setOutputKeyClass(Text.class);
        job_two.setOutputValueClass(Text.class);
        //job_two.setPartitionerClass(MyPartitioner.class);
         
        job_two.setMapperClass(mapTwo.class);
        job_two.setReducerClass(reduceTwo.class);
        
        
        // Partitioner is our custom partitioner class
        job_two.setPartitionerClass(MyPartitioner.class);
        
        // Input and output format class
        job_two.setInputFormatClass(KeyValueTextInputFormat.class);
        job_two.setOutputFormatClass(TextOutputFormat.class);
         
        // The output of previous job set as input of the next
        FileInputFormat.addInputPath(job_two, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job_two, new Path(otherArgs[1]));
        
        // Run the job
 		job_two.waitForCompletion(true);
 		
 		//Remove the temporary file
 		fs.delete(new Path("/labs/lab3/exp2/temp"), true);
	}
	
	public static class mapOne extends Mapper<Text, Text, Text, IntWritable> {
		private int count = 0;
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			//Check that the amount of samples taken is less than the total sample number
			if(count < totalSamples) {
				//Output the first character of the key
				String keyString = key.toString();
				char firstChar = keyString.charAt(0);
				context.write(new Text("" + firstChar), new IntWritable(1));
				count++;
			}
		}
	}

	public static class reduceOne extends Reducer<Text, IntWritable, IntWritable, Text> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			
			//For each character get its count
			for(IntWritable value : values) {
				count += value.get();
			}
			context.write(new IntWritable(count), key);
        }
	} 
	
	// Compare each input key with the boundaries we get from the first round
	// And add the partitioner information in the end of values
	public static class mapTwo extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			int firstChar = (int)key.toString().charAt(0);
			
			//Check the what boundary the data is part of
			//Output the data's key as the key with the boundary added to the end of the payload to be used by the partitioner
			if(firstChar >= boundaries[0][0] && firstChar <= boundaries[0][1]) {
				context.write(key, new Text(value.toString() + ";" + "0"));
			} 
			else if(firstChar >= boundaries[1][0] && firstChar <= boundaries[1][1]) {
				context.write(key, new Text(value.toString() + ";" + "1"));
			} 
			else if(firstChar >= boundaries[2][0] && firstChar <= boundaries[2][1]) {
				context.write(key, new Text(value.toString() + ";" + "2"));
			} 
			else if(firstChar >= boundaries[3][0] && firstChar <= boundaries[3][1]) {
				context.write(key, new Text(value.toString() + ";" + "3"));
			} 
			else if(firstChar >= boundaries[4][0] && firstChar <= boundaries[4][1]) {
				context.write(key, new Text(value.toString() + ";" + "4"));
			} 
			else if(firstChar >= boundaries[5][0] && firstChar <= boundaries[5][1]) {
				context.write(key, new Text(value.toString() + ";" + "5"));
			} 
			else if(firstChar >= boundaries[6][0] && firstChar <= boundaries[6][1]) {
				context.write(key, new Text(value.toString() + ";" + "6"));
			} 
			else if(firstChar >= boundaries[7][0] && firstChar <= boundaries[7][1]) {
				context.write(key, new Text(value.toString() + ";" + "7"));
			} 
			else if(firstChar >= boundaries[8][0] && firstChar <= boundaries[8][1]) {
				context.write(key, new Text(value.toString() + ";" + "8"));
			} 
			else if(firstChar >= boundaries[9][0]) {
				context.write(key, new Text(value.toString() + ";" + "9"));
			} 
		}
	}
	
	public static class reduceTwo extends Reducer<Text, Text, Text, Text> {
     	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
     		//Output the sorted data, removing the boundary identifiers at the end
     		for (Text value : values) {
     			String stringVal = value.toString();
     			String[] splitStringVal = stringVal.split(";");
				context.write(key, new Text(splitStringVal[0]));
			}
     	}
	 }
	
	// Extract the partitioner information from the input values, which decides the destination of data
	public static class MyPartitioner extends Partitioner<Text,Text>{
	   public int getPartition(Text key, Text value, int numReduceTasks){
		   String[] desTmp = value.toString().split(";");
		   return Integer.parseInt(desTmp[1]);
	   }
	}
}
