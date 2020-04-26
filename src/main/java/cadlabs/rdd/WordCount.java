package cadlabs.rdd;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class WordCount {
	
	private static final String DEFAULT_FILE = "data/hamlet.txt";

	public static JavaPairRDD<String, Integer> wordCount(JavaRDD<String> file) {

		JavaRDD<String> words = file.flatMap(s -> 
			Arrays.asList(s.toLowerCase().split("\\s+")).iterator());
		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
		
		JavaPairRDD<String, Integer> sorted =  // sort by value
				counts.mapToPair(x -> x.swap()).sortByKey(false).
				mapToPair(x -> x.swap());
				
		return sorted;
	}

	
	public static void main(String[] args) {
		
		String file;
		if (args.length < 1) {
			System.err.println("Usage: WordCount <file>");
			System.err.println("No file given, assuming " + DEFAULT_FILE);
			file = DEFAULT_FILE;
		}
		else
			file = args[0];


		// start Spark session (SparkContext API may also be used) 
		// master("local") indicates local execution
		SparkSession spark = SparkSession.builder().
				appName("WordCount").
				master("local").
				getOrCreate();
		
		// only error messages are logged from this point onward
		// comment (or change configuration) if you want the entire log
		spark.sparkContext().setLogLevel("ERROR");

		JavaRDD<String> textFile = spark.read().textFile(file).javaRDD();

		JavaPairRDD<String, Integer> counts =  wordCount(textFile);
			
		
		for (Tuple2<?, ?> tuple : counts.take(20)) 
			System.out.println(tuple._1() + ": " + tuple._2());

		spark.stop();

	}

}

