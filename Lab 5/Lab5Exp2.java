import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Lab5Exp2 {
	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("WordCount in Spark");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> ips = context.textFile("/data/ip_trace");
		JavaRDD<String> blocks = context.textFile("/data/raw_block");
		
		//Convert the IPs to A.B.C.D format
		JavaRDD<String> removeFourth = ips.map(new Function<String, String>() {
			public String call(String s) {
				String[] split = s.split(" ");
				split[2] = split[2].substring(0, split[2].lastIndexOf("."));
				split[4] = split[4].substring(0, split[4].lastIndexOf("."));
				return String.join(" ", split);
			}
		});
		
		//Remove any non blocked connections
		JavaRDD<String> removeNonBlocked = blocks.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("Blocked");
			}
		});
		
		//Convert the IPs RDD to a <Key, Value> pair RDD with the Connection ID as the key
		JavaPairRDD<Integer, String> pairIps = removeFourth.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String s) {
				String[] split = s.split(" ");
				return new Tuple2<Integer, String>(Integer.parseInt(split[1]), split[0] + " " + split[1] + " " + split[2] + " " + split[4]);
			}
		});
		
		//Convert the Blocks RDD to a <Key, Value> pair RDD with the Connection ID as the key
		JavaPairRDD<Integer, String> pairBlocks = removeNonBlocked.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String s) {
				String[] split = s.split(" ");
				return new Tuple2<Integer, String>(Integer.parseInt(split[0]), split[1]);
			}
		});
		
		//Join the two RDDs on the Connection ID and sort based on the key
		JavaPairRDD<Integer, Tuple2<String, String>> joined = pairIps.join(pairBlocks);
		JavaPairRDD<Integer, Tuple2<String, String>> sortedJoined = joined.sortByKey();
		
		//Convert the Pair RDD to a regular RDD and save to a file
		JavaRDD<String> firewallLog = sortedJoined.map(new Function<Tuple2<Integer, Tuple2<String, String>>, String>() {
			public String call(Tuple2<Integer, Tuple2<String, String>> t) {
				Tuple2<String, String> firewallData = t._2();
				return firewallData._1() + " " + firewallData._2();
			}
		});
		firewallLog.saveAsTextFile("/labs/lab5/exp2/temp");
		
		//Covert the Fire-wall Log to a <Key, Value> pair RDD with the Source IP being the key
		JavaPairRDD<String, Integer> uniqueSources = firewallLog.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				String[] split = s.split(" ");
				return new Tuple2<String, Integer>(split[2], 1);
			}
		});
		
		//Get the count of each Source IP
		JavaPairRDD<String, Integer> counts = uniqueSources.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}, numOfReducers);
		
		//Sort by the number of occurrences
		JavaPairRDD<Integer, String> flipped = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) {
				return new Tuple2<Integer, String>(t._2(), t._1());
			}
		});
		JavaPairRDD<Integer, String> sorted = flipped.sortByKey(false);
		
		sorted.saveAsTextFile("/labs/lab5/exp2/output");
		context.stop();
		context.close();
	}
}