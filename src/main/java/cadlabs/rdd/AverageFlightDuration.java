package cadlabs.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Map;

public class AverageFlightDuration extends AbstractFlightAnalyser<Map<Tuple2<String, String>, Double>> {

    private static final int MAX_TIME = 60 * (2400 / 100) + 2400 % 100; //1440

    public AverageFlightDuration(JavaRDD<Flight> flights) {
        super(flights);
    }

    /**
     * @param flights
     * @return
     */
    public static JavaPairRDD<Tuple2<String, String>, Double> getAverageDistances(JavaRDD<Flight> flights) {
        JavaPairRDD<Tuple2<String, String>, Double> temp = flights.mapToPair(
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

    /**
     * @param flights
     * @return
     */
    public static JavaPairRDD<Tuple2<Long, Long>, Double> getAverageDistancesById(JavaRDD<Flight> flights) {
        JavaPairRDD<Tuple2<Long, Long>, Tuple2<Integer, Integer>> intTimes =
                flights.mapToPair(
                        f -> new Tuple2<>(
                                new Tuple2<>(f.org_id, f.dest_id),
                                new Tuple2<>((int) f.deptime, (int) f.arrtime)));

        JavaPairRDD<Tuple2<Long, Long>, Tuple2<Integer, Integer>> convertedTimes =
                intTimes.mapToPair(f -> new Tuple2<>(f._1, new Tuple2<>(
                        60 * (f._2._1 / 100) + f._2._1 % 100,
                        60 * (f._2._2 / 100) + f._2._2 % 100)));

        JavaPairRDD<Tuple2<Long, Long>, Tuple2<Double, Long>> recordedFlights =
                convertedTimes.mapToPair(f -> {
                    double duration = f._2._2 - f._2._1;
                    if (duration <= 0) duration = MAX_TIME - f._2._1 + f._2._2;
                    return new Tuple2<>(f._1, new Tuple2<>(duration, 1L));
                });

        JavaPairRDD<Tuple2<Long, Long>, Tuple2<Double, Long>> allDistanceSamples =
                recordedFlights.union(
                        recordedFlights.mapToPair(
                                f -> new Tuple2<>(
                                        new Tuple2<>(f._1._2, f._1._1), f._2)));

        return allDistanceSamples.reduceByKey((v1, v2) ->
                new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2))
                .mapValues(v1 -> v1._1 / v1._2);
    }

    // Method that aggregates every
    // flight through it's route
    // and then aggregates it through their
    // value
    @Override
    public Map<Tuple2<String, String>, Double> run() {
        return getAverageDistances(flights).collectAsMap();
    }
}
