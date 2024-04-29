import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Lab6Exp1 {
	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordCount <input> <output>");
			System.exit(1);
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("GitHub Repo Counter");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> github = context.textFile(args[0]);
		
		//Convert GitHub file to <Key, Value> pair RDD with the language as the id
		JavaPairRDD<String, Integer> languages = github.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				String[] split = s.split(",");
				return new Tuple2<String, Integer>(split[1], 1);
			}
		});
		
		//Get the count of each language
		JavaPairRDD<String, Integer> languageCounts = languages.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}, numOfReducers);
		
		//Convert GitHub file to <Key, Value> pair RDD with the language as the id and Value as the needed info
		JavaPairRDD<String, String> languagesWithInfo = github.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) {
				String[] split = s.split(",");
				return new Tuple2<String, String>(split[1], split[0] + " " + split[12]);
			}
		});
		
		//Joining the counts with the rest of the info
		JavaPairRDD<String, Tuple2<Integer, String>> joined = languageCounts.join(languagesWithInfo);
		
		//Get the highest stars for each language
		JavaPairRDD<String, Tuple2<Integer, String>> maxStars = joined.reduceByKey(new Function2<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>>() {
			public Tuple2<Integer, String> call(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
				if(Integer.parseInt(t1._2().split(" ")[1]) > Integer.parseInt(t2._2().split(" ")[1])) {
					return t1;
				}
				else {
					return t2;
				}
			}
		}, 1);
		
		//Sort by the number of repos
		JavaPairRDD<Integer, String> countAsKey = maxStars.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, String>>, Integer, String>() {
			public Tuple2<Integer, String> call(Tuple2<String, Tuple2<Integer, String>> t) {
				int repoCount = t._2()._1();
				String info = t._1() + " " + t._2()._2();
				return new Tuple2<Integer, String>(repoCount, info);
			}
		});
		JavaPairRDD<Integer, String> sorted = countAsKey.sortByKey(false);
		
		//Format the output
		JavaRDD<String> githubInfo = sorted.map(new Function<Tuple2<Integer, String>, String>() {
			public String call(Tuple2<Integer, String> t) {
				String[] split = t._2().split(" ");
				return split[0] + " " + t._1() + " " + split[1] + " " + split[2];
			}
		});
		
		githubInfo.saveAsTextFile(args[1]);
		context.stop();
		context.close();
	}
}