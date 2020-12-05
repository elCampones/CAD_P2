package cadlabs.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Map;

public class RouteWithMoreFlights extends AbstractFlightAnalyser<Tuple2<Tuple2<String, String>, Long>> {

    public RouteWithMoreFlights(JavaRDD<Flight> flights) {
        super(flights);
    }

    @Override
    public Tuple2<Tuple2<String, String>, Long> run() {
        // This maps all the flights by their
        // origin / destination pair
        return this.flights.mapToPair(
                        flight -> new Tuple2<>(new Tuple2<>(flight.origin, flight.dest), 1L)
                ).reduceByKey(
                        (v1, v2) -> v1 + v2
                ).reduce(
                        (v1, v2) -> v2._2 >= v1._2 ? v2 : v1
                );

//        return flightsMappedByRoute.reduce(
//                (v1, v2) -> v2._2 >= v1._2 ? v2 : v1
//        );
    }
}
