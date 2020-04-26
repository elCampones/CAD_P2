package cadlabs.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class AbstractFlightAnalyser<T> {

	protected final Dataset<Row> flights;
	
	public AbstractFlightAnalyser(Dataset<Row> flights) {
		this.flights = flights;
	}
	
	
	public abstract T run();
	
}
