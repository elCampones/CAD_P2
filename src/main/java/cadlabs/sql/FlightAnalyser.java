package cadlabs.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FlightAnalyser {

	private static final String DefaulftFile = "data/flights.csv";


	public static void main(String[] args) {
		
		String file = (args.length < 1) ? DefaulftFile : args[0];
		
		// start Spark session (SparkContext API may also be used) 
		// master("local") indicates local execution
		SparkSession spark = SparkSession.builder().
				appName("FlightAnalyser").
				master("local[*]").
				getOrCreate();
		
		// only error messages are logged from this point onward
		// comment (or change configuration) if you want the entire log
		spark.sparkContext().setLogLevel("ERROR");

		
		Dataset<String> textFile = spark.read().textFile(file).as(Encoders.STRING());	
		Dataset<Row> flights = 
				textFile.map((MapFunction<String, Row>) l -> Flight.parseFlight(l), 
				Flight.encoder()).cache();

		//TODO
		
		
		for (Row r : flights.collectAsList())
			System.out.println(r);
		
		
		// terminate the session
		spark.stop();
	}

}
