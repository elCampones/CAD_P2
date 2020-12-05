package cadlabs.rdd;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class FlightsPerAirport extends AbstractFlightAnalyser<Map<String, Long>> {

	
	public FlightsPerAirport(JavaRDD<Flight> flights) {
		super(flights);
	}
	
	
	public Map<String, Long> run() {
		JavaPairRDD<String, String> allRoutes = 
					this.flights.mapToPair(flight -> new Tuple2<>(flight.origin, flight.dest));
		JavaPairRDD<String, Long> origins = allRoutes.mapToPair(routes -> new Tuple2<>(routes._1, 1L));
		JavaPairRDD<String, Long> dests = allRoutes.mapToPair(routes -> new Tuple2<>(routes._2, 1L));
		JavaPairRDD<String, Long> airports = origins.union(dests);
		JavaPairRDD<String, Long> counts = airports.reduceByKey((i1,i2) -> i1 + i2);
		JavaPairRDD<String, Long> sorted = counts.mapToPair(count -> count.swap())
				.sortByKey(false).mapToPair(count -> count.swap());
		return sorted.collectAsMap();
	}

}
