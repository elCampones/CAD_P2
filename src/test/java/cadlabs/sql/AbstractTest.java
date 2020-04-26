package cadlabs.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import cadlabs.AbstractBaseTest;


public abstract class AbstractTest<Result>  extends AbstractBaseTest<Dataset<Row>, Result> {

	
	protected Dataset<Row> processInputFile(String file) {
		Dataset<String> textFile = spark.read().textFile(file).as(Encoders.STRING());	
		Dataset<Row> flights = 
				textFile.map((MapFunction<String, Row>) l -> Flight.parseFlight(l), 
				Flight.encoder()).cache();
		
		return flights;
	}
	
}
