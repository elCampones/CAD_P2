package cadlabs.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Map;

public class AverageFlightDuration  extends AbstractFlightAnalyser<Map<Tuple2<String, String>, Double>> {

    public AverageFlightDuration(JavaRDD<Flight> flights) {
        super(flights);
    }

    static public JavaPairRDD<Tuple2<String, String>, Double> getAverageDistances (JavaRDD<Flight> flights) {
        JavaPairRDD<Tuple2<String, String>, Double> temp =  flights.mapToPair(
                flight -> new Tuple2<>(new Tuple2<>(flight.origin, flight.dest), new Tuple2<>(flight.arrtime - flight.deptime, 1L))
        ).reduceByKey(
                (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)
        ).mapValues(
                v1 -> v1._1 / v1._2
        );

        JavaPairRDD<Tuple2<String, String>, Double> reverseCombinations =
                temp.mapToPair(f -> new Tuple2<>(new Tuple2<>(f._1._2, f._1._1), f._2));

        return temp.union(reverseCombinations);
    }

    static public JavaPairRDD<Tuple2<Long, Long>, Double> getAverageDistancesById (JavaRDD<Flight> flights) {
        JavaPairRDD<Tuple2<Long, Long>, Double> temp =  flights.mapToPair(
                flight -> new Tuple2<>(new Tuple2<>(flight.org_id, flight.dest_id), new Tuple2<>(flight.arrtime - flight.deptime, 1L))
        ).reduceByKey(
                (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)
        ).mapValues(
                v1 -> v1._1 / v1._2
        );

        JavaPairRDD<Tuple2<Long, Long>, Double> reverseCombinations =
                temp.mapToPair(f -> new Tuple2<>(new Tuple2<>(f._1._2, f._1._1), f._2));

        return temp.union(reverseCombinations);
    }

    // Method that aggregates every
    // flight through it's route
    // and then aggregates it through their
    // value
    @Override
    public Map<Tuple2<String, String>, Double> run() {
        // TODO : Not sure if this is realy
        //  efficient
        return getAverageDistances(flights).collectAsMap();
    }
}
