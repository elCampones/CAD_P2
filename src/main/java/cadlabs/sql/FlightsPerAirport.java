package cadlabs.sql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;


public class FlightsPerAirport extends AbstractFlightAnalyser<List<Row>> {


    public FlightsPerAirport(Dataset<Row> flights) {
        super(flights);
    }


    public List<Row> run() {
        Dataset<Row> counts = this.flights.groupBy("origin").count();


        return counts.collectAsList();
    }

}
