package cadlabs.sql;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


public class WordCount {

	private static final String DEFAULT_FILE = "data/hamlet.txt";
	
	
	public static Dataset<Row> wordCount(Dataset<String> data) {

		 Dataset<String> words = data.flatMap(
				 	(FlatMapFunction<String, String>) s ->  Arrays.asList(s.toLowerCase().split(" ")).iterator(), 
				 	Encoders.STRING());
		 
	//	 words.printSchema();
		 
		 Dataset<Row> result = words.groupBy("value").count();
	//	 result.printSchema();
		 		 
		return result.sort(functions.desc("count"));
		 
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
				appName("WordCount").master("local").getOrCreate();
		
		// only error messages are logged from this point onward
		// comment (or change configuration) if you want the entire log
		spark.sparkContext().setLogLevel("ERROR");

		Dataset<String> textFile = spark.read().textFile(file).as(Encoders.STRING());
		Dataset<Row> counts =  wordCount(textFile);

		// print all as list
//		System.out.println(counts.collectAsList());
		
		int i = 1;
		for (Row r : (Row[]) counts.take(20))
			System.out.println((i++) + ": " + r);
	
	
		spark.stop();

	}

}
