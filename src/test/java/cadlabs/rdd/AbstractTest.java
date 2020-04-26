package cadlabs.rdd;

import org.apache.spark.api.java.JavaRDD;

import cadlabs.AbstractBaseTest;


public abstract class AbstractTest<Result> extends AbstractBaseTest<JavaRDD<Flight>, Result> {

	
	protected JavaRDD<Flight>  processInputFile(String file) {
		JavaRDD<String> textFile = spark.read().textFile(file).javaRDD();
		JavaRDD<Flight> flights = textFile.map(l -> Flight.parseFlight(l)).cache();
		
		return flights;
	}
	

}
