import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class Lab6Exp2 {
	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordCount <input> <output>");
			System.exit(1);
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("GitHub Repo Counter");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> graph = context.textFile(args[0]);
		
		//Remove any blank lines
		JavaRDD<String> graphNoBlank = graph.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return !s.isEmpty();
			}
		});

		//Make a <Key, Value> pair RDD with Vertex 1 as the key and Vertex 2 as the value
		JavaPairRDD<Integer, Integer> edges = graphNoBlank.flatMapToPair(new PairFlatMapFunction<String, Integer, Integer>() {
			public Iterator<Tuple2<Integer, Integer>> call(String s) {
				String[] split = s.split("\\s+");
				List<Tuple2<Integer, Integer>> edgeList = new ArrayList<Tuple2<Integer, Integer>>();
				edgeList.add(new Tuple2<Integer, Integer>(Integer.parseInt(split[0]), Integer.parseInt(split[1])));
				edgeList.add(new Tuple2<Integer, Integer>(Integer.parseInt(split[1]), Integer.parseInt(split[0])));
				return edgeList.iterator();
			}
		});
		
		//Get the neighbors of each vertex
		JavaPairRDD<Integer, Iterable<Integer>> neighbors = edges.groupByKey();
		
		//Make potential triangles out of the neighbors
		JavaRDD<String> triangles = neighbors.flatMap(new FlatMapFunction<Tuple2<Integer, Iterable<Integer>>, String>() {
			public Iterator<String> call(Tuple2<Integer, Iterable<Integer>> t) {
				List<Integer> neighborList = new ArrayList<Integer>();
				for(Integer vertex : t._2()) {
					neighborList.add(vertex);
				}
		
				List<String> triangleList = new ArrayList<String>();
				
				//Iterate over the list of neighbors twice to make the triangles
				for(int i = 0; i < neighborList.size(); i++) {
					for(int j = i + 1; j < neighborList.size(); j++) {
						//Sort the triangles to make sure duplicates are the same
						int[] triangle = {t._1(), neighborList.get(i), neighborList.get(j)};
						Arrays.sort(triangle);
						String triangleString = triangle[0] + " " + triangle[1] + " " + triangle[2];
						triangleList.add(triangleString);
					}
				}
				return triangleList.iterator();
			}
		});
		
		//Count how many of each triangle there is
		JavaPairRDD<String, Integer> ones = triangles.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}, numOfReducers);

		//Keep only the real triangles
		JavaPairRDD<String, Integer> realTriangles = counts.filter(new Function<Tuple2<String, Integer>, Boolean>() {
			public Boolean call(Tuple2<String, Integer> t) {
				//If there are 3 of more of a triangle that makes it a real triangle
				return t._2() >= 3;
			}
		});
		
		//Output the amount of triangles
		List<String> countOfTriangles = new ArrayList<String>();
		countOfTriangles.add("Total Triangles: " + realTriangles.count());
		JavaRDD<String> output = context.parallelize(countOfTriangles);
		
		output.saveAsTextFile(args[1]);
		context.stop();
		context.close();
	}
}