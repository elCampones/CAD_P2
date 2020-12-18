package cadlabs.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Map;

public class FlightsPerRoute extends AbstractFlightAnalyser<Map<Tuple2<String, String>, Long>> {

    public FlightsPerRoute(JavaRDD<Flight> flights) {
        super(flights);
    }

    @Override
    public Map<Tuple2<String, String>, Long> run() {
        JavaPairRDD<String, String> allRoutes =
                this.flights.mapToPair(flight -> new Tuple2<>(flight.origin, flight.dest));
        JavaPairRDD<Tuple2<String, String>, Long> routesNumber =
                allRoutes.mapToPair(route -> new Tuple2<Tuple2<String, String>, Long>(new Tuple2<>(route._1, route._2), 1L));
        JavaPairRDD<Tuple2<String, String>, Long> counts =
                routesNumber.reduceByKey((i1, i2) -> i1 + i2);
        JavaPairRDD<Tuple2<String, String>, Long> sorted =
                counts.mapToPair(p -> p.swap()).sortByKey(false).mapToPair(p -> p.swap());
        for (Tuple2<Tuple2<String, String>, Long> t : sorted.take(20))
            System.out.printf("(%s, %s) = %d", t._1._1, t._1._2, t._2);
        return sorted.collectAsMap();
    }

}
