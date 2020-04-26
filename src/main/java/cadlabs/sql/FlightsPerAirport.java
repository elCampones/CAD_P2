package cadlabs.sql;



import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;



public class FlightsPerAirport extends AbstractFlightAnalyser<List<Row>> {

	
	public FlightsPerAirport(Dataset<Row> flights) {
		super(flights);
	}
	
	
	public List<Row> run() {
		Dataset<Row> counts = this.flights.groupBy("origin").count();
			
		return counts.collectAsList();
	}

}
