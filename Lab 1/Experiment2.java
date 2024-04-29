import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class Experiment2 {
	public static void main(String[] args) throws Exception {
		//System configuration
		Configuration conf = new Configuration();
		
		//Set up the file system
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/lab1/bigdata");
		FSDataInputStream inputStream = fs.open(path);
		
		//Get the checksum value by seeking to the 1000000000 bit then reading for the next 1000
		int checksum = 0;
		inputStream.seek(1000000000);
		for(int i = 0; i < 1000; i++) {
			int currentByte = inputStream.read();
			checksum ^= currentByte;
		}
		
		//Print and close the file
		System.out.println("Checksum value is " + checksum);
		fs.close();
	}
}
