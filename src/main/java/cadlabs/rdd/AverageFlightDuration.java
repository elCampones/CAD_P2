package cadlabs.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Map;

public class AverageFlightDuration  extends AbstractFlightAnalyser<Map<Tuple2<String, String>, Double>> {

    public AverageFlightDuration(JavaRDD<Flight> flights) {
        super(flights);
    }

    // Method that aggregates every
    // flight through it's route
    // and then aggregates it through their
    // value
    @Override
    public Map<Tuple2<String, String>, Double> run() {
        JavaPairRDD<Tuple2<String, String>, Double> temp =
                this.flights.mapToPair(
                        flight -> new Tuple2<>(new Tuple2<>(flight.origin, flight.dest), new Tuple2<>(flight.arrtime - flight.deptime, 1L))
                ).reduceByKey(
                        (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)
                ).mapValues(
                        v1 -> v1._1 / v1._2
                );
        return temp.collectAsMap();
    }
}
