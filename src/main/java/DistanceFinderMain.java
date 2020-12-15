import cadlabs.par.ParSSSP;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class DistanceFinderMain {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java DistanceFinderMain Origin Destiny [FlightsFile]\n "
            		+ "Origin and Destiny: names of airports\n"
            		+ "FlightsFile: Pathname of the file containing the flights data\s");
            System.exit(0);
        }
        String originStr = args[0], destStr = args[1];

        // start Spark session (SparkContext API may also be used)
        // master("local") indicates local execution
        SparkSession spark = SparkSession.builder().
                appName("FlightAnalyser").
                master("local").
                getOrCreate();


        // only error messages are logged from this point onward
        // comment (or change configuration) if you want the entire log
        spark.sparkContext().setLogLevel("ERROR");

        String file = args.length > 3 ? args[2] : "data/flights.csv";

        JavaRDD<Flight> flights = processInputFile(file, spark);

        long start = System.currentTimeMillis();
        Path path = new ParSSSP(originStr, destStr, flights).run();
        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("Path from %s to %s: %s\n"
        		+ "Computed in %d milliseconds"
                , originStr, destStr, path, elapsed);
    }

    private static JavaRDD<Flight> processInputFile(String file, SparkSession spark) {
        JavaRDD<String> textFile = spark.read().textFile(file).javaRDD();
        return textFile.map(Flight::parseFlight).cache();
    }

}
